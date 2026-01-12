use crate::types::{Side, Tick};
use arrow::array::{Array, Int64Array, Int8Array};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::RecordBatch;


/// A source of ticks for a single (exchange, symbol) stream.
///
/// This abstraction allows the engine to be agnostic to whether ticks come from
/// a localized Vec, a CSV file, or an Arrow stream.
pub trait TickSource: Send {
    /// Peek at the next available tick without consuming it.
    /// Returns None if the stream is exhausted.
    fn peek(&mut self) -> Option<&Tick>;

    /// Consume the next tick.
    /// Returns None if the stream is exhausted.
    fn next(&mut self) -> Option<Tick>;

    /// Returns the symbol_id associated with this source.
    fn symbol_id(&self) -> u32;
}

pub struct ArrowTickSource {
    symbol_id: u32,
    reader: ArrowArrayStreamReader,
    current_batch: Option<RecordBatch>,
    batch_idx: usize,
    next_tick: Option<Tick>,
}

impl ArrowTickSource {
    pub fn new(symbol_id: u32, reader: ArrowArrayStreamReader) -> Self {
        let mut source = Self {
            symbol_id,
            reader,
            current_batch: None,
            batch_idx: 0,
            next_tick: None,
        };
        // Pre-load the first tick
        source.advance();
        source
    }

    fn advance(&mut self) {
        loop {
            // If we have a current batch, try to read the next row
            if let Some(batch) = &self.current_batch {
                if self.batch_idx < batch.num_rows() {
                    self.next_tick = Some(self.read_tick_at(batch, self.batch_idx));
                    self.batch_idx += 1;
                    return;
                }
            }

            // No current batch or batch exhausted, try to load next batch
            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.batch_idx = 0;
                    continue;
                }
                Some(Err(e)) => {
                    eprintln!("Error reading arrow stream: {}", e);
                    self.next_tick = None;
                    return;
                }
                None => {
                    self.current_batch = None;
                    self.next_tick = None;
                    return;
                }
            }
        }
    }

    fn read_tick_at(&self, batch: &RecordBatch, idx: usize) -> Tick {
        // Schema assumptions (will need robust validation in production):
        // ts_exchange: Int64
        // price: Int64
        // qty: Int64 (or "size")
        // side: Int8
        // seq: Int64 (optional)
        // ts_local: Int64 (optional)
        


        let ts_exchange_arr = batch.column_by_name("ts_exchange")
            .expect("missing ts_exchange")
            .as_any().downcast_ref::<Int64Array>().expect("ts_exchange not Int64");
        
        let price_arr = batch.column_by_name("price")
            .expect("missing price")
            .as_any().downcast_ref::<Int64Array>().expect("price not Int64");

        // qty or size
        let qty_arr = batch.column_by_name("qty")
            .or_else(|| batch.column_by_name("size"))
            .expect("missing qty/size")
            .as_any().downcast_ref::<Int64Array>().expect("qty not Int64");
            
        let side_arr = batch.column_by_name("side")
            .expect("missing side")
            .as_any().downcast_ref::<Int8Array>().expect("side not Int8");

        let ts_exchange = ts_exchange_arr.value(idx);
        let price = price_arr.value(idx);
        let qty = qty_arr.value(idx);
        let side_val = side_arr.value(idx);
        let side = Side::try_from(side_val).expect("invalid side");
        
        let seq = if let Some(col) = batch.column_by_name("seq") {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                arr.value(idx) as u64
            } else {
                0
            }
        } else {
            0
        };

        let ts_local = if let Some(col) = batch.column_by_name("ts_local") {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                arr.value(idx)
            } else {
                0
            }
        } else {
            0
        };

        Tick {
            ts_exchange,
            ts_local,
            seq,
            symbol_id: self.symbol_id,
            price,
            qty,
            side,
            flags: 0, 
        }
    }
}

impl TickSource for ArrowTickSource {
    fn peek(&mut self) -> Option<&Tick> {
        self.next_tick.as_ref()
    }

    fn next(&mut self) -> Option<Tick> {
        let tick = self.next_tick.take();
        if tick.is_some() {
            self.advance();
        }
        tick
    }

    fn symbol_id(&self) -> u32 {
        self.symbol_id
    }
}
