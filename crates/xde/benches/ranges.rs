use xde::{ByteRange, RangeSet};

#[divan::bench]
fn range_insert_merge(bencher: divan::Bencher) {
    bencher.bench(|| {
        let mut s = RangeSet::new();
        for i in 0..256u64 {
            s.insert(ByteRange::new(i * 10, i * 10 + 6));
        }
        s.covered_len()
    });
}

fn main() {
    divan::main();
}
