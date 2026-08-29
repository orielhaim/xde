//! Shard-local HTTP/2 connection task.
//!
//! Hyper's H2 dispatcher, the h2 I/O driver (queued through [`H2Exec`]), and
//! every admitted stream future are polled with the same [`Context`]. DATA
//! reads, WINDOW_UPDATE writes, and body consumption share one Compio waker so
//! the runtime never parks on the Windows timer quantum for a cold sibling.

use std::{
    cell::RefCell,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// Cloneable handle used to admit stream work and to implement Hyper's
/// executor so ConnTask/Pipe/SendWhen stay on this connection.
#[derive(Clone)]
pub struct H2Handle {
    inner: Rc<RefCell<H2Inner>>,
}

struct H2Inner {
    dispatcher: Option<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    io: Vec<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    streams: Vec<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    waker: Option<Waker>,
    closed: bool,
}

impl H2Handle {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(H2Inner {
                dispatcher: None,
                io: Vec::new(),
                streams: Vec::new(),
                waker: None,
                closed: false,
            })),
        }
    }

    pub fn executor(&self) -> H2Exec {
        H2Exec(self.clone())
    }

    pub fn set_dispatcher<F>(&self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.inner.borrow_mut().dispatcher = Some(Box::pin(fut));
        self.wake();
    }

    /// Run `fut` on this connection's task. Payload bytes stay inside `fut`.
    pub fn admit<F>(&self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.inner.borrow_mut().streams.push(Box::pin(fut));
        self.wake();
    }

    pub fn task(&self) -> H2ConnectionTask {
        H2ConnectionTask {
            inner: self.inner.clone(),
        }
    }

    pub fn shutdown(&self) {
        self.inner.borrow_mut().closed = true;
        self.wake();
    }

    fn wake(&self) {
        if let Some(waker) = self.inner.borrow().waker.clone() {
            waker.wake();
        }
    }

    /// Drive dispatcher, h2 I/O, and admitted streams with `cx`.
    pub fn poll_task(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.inner.borrow_mut().waker = Some(cx.waker().clone());

        poll_dispatcher(&self.inner, cx);
        poll_io(&self.inner, cx);

        let mut streams = std::mem::take(&mut self.inner.borrow_mut().streams);
        streams.retain_mut(
            |fut| match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
                Ok(Poll::Pending) => true,
                Ok(Poll::Ready(())) => false,
                Err(_) => {
                    tracing::error!(
                        target: "xde::net",
                        "h2 stream task panicked; sibling streams continue"
                    );
                    false
                }
            },
        );
        self.inner.borrow_mut().streams.append(&mut streams);

        // Consume-then-drive: WINDOW_UPDATE and the next read live on
        // Hyper's Connection future (the dispatcher), not only on executor
        // Pipe tasks. Skipping this poll leaves no IOCP and Compio parks
        // on the Windows timer quantum (~16 ms).
        poll_dispatcher(&self.inner, cx);
        poll_io(&self.inner, cx);

        let inner = self.inner.borrow();
        if inner.closed && inner.streams.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn push_io(&self, fut: Pin<Box<dyn Future<Output = ()> + 'static>>) {
        self.inner.borrow_mut().io.push(fut);
        self.wake();
    }
}

/// Hyper executor that queues work onto [`H2Handle`] instead of spawning.
#[derive(Clone)]
pub struct H2Exec(H2Handle);

impl<F> hyper::rt::Executor<F> for H2Exec
where
    F: Future<Output = ()> + 'static,
{
    fn execute(&self, fut: F) {
        self.0.push_io(Box::pin(fut));
    }
}

/// The single Compio task for one physical H2 connection.
pub struct H2ConnectionTask {
    inner: Rc<RefCell<H2Inner>>,
}

impl Future for H2ConnectionTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        H2Handle {
            inner: self.inner.clone(),
        }
        .poll_task(cx)
    }
}

fn poll_dispatcher(inner: &Rc<RefCell<H2Inner>>, cx: &mut Context<'_>) {
    let mut dispatcher = inner.borrow_mut().dispatcher.take();
    if let Some(fut) = dispatcher.as_mut()
        && fut.as_mut().poll(cx).is_pending()
    {
        inner.borrow_mut().dispatcher = dispatcher;
    }
}

fn poll_io(inner: &Rc<RefCell<H2Inner>>, cx: &mut Context<'_>) {
    let mut io = std::mem::take(&mut inner.borrow_mut().io);
    io.retain_mut(|fut| fut.as_mut().poll(cx).is_pending());
    inner.borrow_mut().io.append(&mut io);
}
