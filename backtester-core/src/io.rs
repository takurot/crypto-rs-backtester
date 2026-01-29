use std::fs::File;
use std::path::Path;
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread;

use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use memmap2::Mmap;

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// A cursor over a memory-mapped file that owns the reference via Arc.
pub struct MmapCursor {
    inner: Arc<Mmap>,
    pos: u64,
}

impl MmapCursor {
    pub fn new(mmap: Arc<Mmap>) -> Self {
        Self {
            inner: mmap,
            pos: 0,
        }
    }
}

impl Read for MmapCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.inner.len() as u64;
        if self.pos >= len {
            return Ok(0);
        }
        let remain = (len - self.pos) as usize;
        let to_read = std::cmp::min(remain, buf.len());

        buf[..to_read].copy_from_slice(&self.inner[self.pos as usize..self.pos as usize + to_read]);
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for MmapCursor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let len = self.inner.len() as u64;
        let new_pos = match pos {
            SeekFrom::Start(p) => p,
            SeekFrom::End(delta) => {
                if delta >= 0 {
                    len.saturating_add(delta as u64)
                } else {
                    len.saturating_sub(delta.wrapping_neg() as u64)
                }
            }
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.pos.saturating_add(delta as u64)
                } else {
                    self.pos.saturating_sub(delta.wrapping_neg() as u64)
                }
            }
        };

        // Clamp to file size (standard behavior allows seeking past end, but let's stick to simple)
        // Actually standard Seek allows past end. But Read returns 0.
        self.pos = new_pos;
        Ok(self.pos)
    }
}

/// A loader that uses memory-mapping to read Arrow IPC streams or files.
///
/// Note: This uses `StreamReader`, so it expects the Arrow IPC Stream format.
/// If you have an Arrow IPC File (Random Access) format, use `FileReader` instead.
pub struct MmapFileLoader {
    mmap: Arc<Mmap>,
}

impl MmapFileLoader {
    /// Open a file and memory-map it.
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: Only safe if the file is not modified while mapped.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            mmap: Arc::new(mmap),
        })
    }

    /// Create an Arrow Stream Reader from the memory-mapped content.
    ///
    /// The returned reader owns a reference to the Mmap, so it is 'static and
    /// can be used with `AsyncBatchIter`.
    pub fn reader(&self) -> Result<StreamReader<MmapCursor>, ArrowError> {
        let cursor = MmapCursor::new(self.mmap.clone());
        StreamReader::try_new(cursor, None)
    }
}

/// An iterator wrapper that pre-fetches items in a background thread.
///
/// This allows I/O (or decompression/parsing) to overlap with consumer processing.
pub struct AsyncBatchIter {
    receiver: Receiver<Option<Result<RecordBatch, ArrowError>>>,
}

impl AsyncBatchIter {
    /// Create a new async iterator with a bounded readahead buffer.
    ///
    /// `inner`: The source iterator (e.g., an Arrow StreamReader).
    /// `readahead`: Number of batches to buffer in the channel.
    pub fn new<I>(inner: I, readahead: usize) -> Self
    where
        I: Iterator<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
    {
        let (sender, receiver) = sync_channel(readahead);

        thread::spawn(move || {
            for item in inner {
                // If the receiver hangs up, we just stop.
                if sender.send(Some(item)).is_err() {
                    return;
                }
            }
            // Signal EOF
            let _ = sender.send(None);
        });

        Self { receiver }
    }
}

impl Iterator for AsyncBatchIter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // block until next item is available
        match self.receiver.recv() {
            Ok(Some(item)) => Some(item),
            Ok(None) => None, // EOF signal
            Err(_) => None,   // Sender hung up (shouldn't happen given logic above)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    // Mock iterator that simulates slow I/O
    struct SlowIter {
        count: usize,
        delay: Duration,
        current: usize,
    }

    impl Iterator for SlowIter {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current >= self.count {
                return None;
            }
            std::thread::sleep(self.delay);
            self.current += 1;

            // Return a dummy batch
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
            let arr = Int64Array::from(vec![self.current as i64]);
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
            Some(Ok(batch))
        }
    }

    #[test]
    #[ignore] // Flaky in CI due to timing checks
    fn test_async_batch_iter_overlap() {
        // 5 items, 50ms each. Serial would take ~250ms.
        // We consume them with 50ms processing time.
        //
        // Serial:
        //   Read 1 (50ms) -> Process 1 (50ms) -> Read 2 (50ms) -> Process 2 (50ms)...
        //   Total approx 500ms.
        //
        // Async (readahead 2):
        //   Read 1 (50ms) starts.
        //   Read 2 (50ms) starts immediately after 1 is pushed? No, sender blocks until read 1 done.
        //   BUT:
        //   T0: Worker starts Read 1. Main waits.
        //   T50: Worker pushes 1. Starts Read 2. Main wakes, starts Process 1.
        //   T100: Worker pushes 2. Starts Read 3. Main finishes Process 1, takes 2, starts Process 2.
        //   ...
        //   Effective throughput is mostly determined by max(Read, Process).
        //   Total time ~ 50 (first read) + 4 * 50 (overlapped) + 50 (last process) = 300ms?

        let count = 5;
        let delay = Duration::from_millis(50);

        let iter = SlowIter {
            count,
            delay,
            current: 0,
        };

        // Create async iter
        let async_iter = AsyncBatchIter::new(iter, 2);

        let start = Instant::now();
        for _batch in async_iter {
            // Simulate processing time
            std::thread::sleep(delay);
        }
        let elapsed = start.elapsed();

        println!("Elapsed: {:?}", elapsed);

        // Serial expectation: (50 read + 50 process) * 5 = 500ms
        // Overlap expectation: 50 (initial read) + 50*4 (overlapped) + 50 (last process) = 300ms
        // Allow some overhead, but it should be significantly less than 500ms.
        assert!(elapsed < Duration::from_millis(450), "Should overlap I/O");
    }

    #[test]
    fn test_async_batch_iter_ordering() {
        let count = 10;
        let delay = Duration::from_micros(100); // fast
        let iter = SlowIter {
            count,
            delay,
            current: 0,
        };
        let async_iter = AsyncBatchIter::new(iter, 5);

        let mut next_val = 1;
        for item in async_iter {
            let batch = item.unwrap();
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), next_val);
            next_val += 1;
        }
        assert_eq!(next_val, 11);
    }
}
