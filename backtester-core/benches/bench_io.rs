use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use backtester_core::io::AsyncBatchIter;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;
use std::time::Duration;

// Simulated slow source
struct MockSource {
    count: usize,
    delay: Duration,
    current: usize,
}

impl Iterator for MockSource {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            return None;
        }
        std::thread::sleep(self.delay); // Simulate I/O latency
        self.current += 1;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![self.current as i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        Some(Ok(batch))
    }
}

fn bench_async_overlap(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_overlap");
    group.sample_size(10); // Slow benchmarks need fewer samples

    // Scenario: I/O (source) takes 10ms, Processing takes 10ms.
    // Serial: 20ms per item.
    // Async: 10ms per item (amortized).
    let delay = Duration::from_millis(10);
    let count = 10;

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("serial_read_process", |b| {
        b.iter(|| {
            let source = MockSource {
                count,
                delay,
                current: 0,
            };
            for _batch in source {
                // Simulate processing
                std::thread::sleep(delay);
            }
        });
    });

    group.bench_function("async_read_process", |b| {
        b.iter(|| {
            let source = MockSource {
                count,
                delay,
                current: 0,
            };
            let async_iter = AsyncBatchIter::new(source, 2);
            for _batch in async_iter {
                // Simulate processing
                std::thread::sleep(delay);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_async_overlap);
criterion_main!(benches);
