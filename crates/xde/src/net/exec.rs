/// hyper's H2 builder wants an executor. Ours just hands the task to the
/// current compio shard, which keeps the connection driver on the same core as
/// its socket.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompioExec;

#[derive(Debug, Clone, Copy, Default)]
pub struct CompioTimer;

struct CompioSleep(
    send_wrapper::SendWrapper<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static>>>,
);

impl std::future::Future for CompioSleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().as_mut().poll(cx)
    }
}

impl hyper::rt::Sleep for CompioSleep {}

impl hyper::rt::Timer for CompioTimer {
    fn sleep(&self, duration: std::time::Duration) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(CompioSleep(send_wrapper::SendWrapper::new(Box::pin(
            compio::time::sleep(duration),
        ))))
    }

    fn sleep_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
        self.sleep(deadline.saturating_duration_since(std::time::Instant::now()))
    }
}

impl<F> hyper::rt::Executor<F> for CompioExec
where
    F: std::future::Future<Output = ()> + 'static,
{
    fn execute(&self, fut: F) {
        compio::runtime::spawn(fut).detach();
    }
}
