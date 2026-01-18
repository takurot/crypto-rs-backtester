use crate::types::{Side, Tick};
use arrow::array::{Array, Int16Array, Int32Array, Int8Array, Int8Builder, Int64Array};
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

struct CachedBatch {
    ts_exchange: Int64Array,
    price: Int64Array,
    qty: Int64Array,
    side: Int8Array,
    seq: Option<Int64Array>,
    ts_local: Option<Int64Array>,
    num_rows: usize,
}

impl CachedBatch {
    fn new(batch: RecordBatch) -> Self {
        let num_rows = batch.num_rows();

        let get_i64 = |name: &str| -> Int64Array {
            batch
                .column_by_name(name)
                .expect(name)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("not Int64")
                .clone()
        };

        // Helper for optional columns or aliases
        let get_i64_opt = |name: &str| -> Option<Int64Array> {
            batch.column_by_name(name).map(|c| {
                c.as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("not Int64")
                    .clone()
            })
        };

        let ts_exchange = batch
            .column_by_name("ts_exchange")
            .or_else(|| batch.column_by_name("ts_event"))
            .expect("missing ts_exchange/ts_event")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts_exchange not Int64")
            .clone();

        let price = get_i64("price");

        let qty = batch
            .column_by_name("qty")
            .or_else(|| batch.column_by_name("size"))
            .expect("missing qty/size")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("qty not Int64")
            .clone();

        let side = {
            let side_col = batch.column_by_name("side").expect("missing side");
            match side_col.data_type() {
                arrow::datatypes::DataType::Int8 => side_col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("side not Int8")
                    .clone(),
                arrow::datatypes::DataType::Int16 => {
                    let arr = side_col
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .expect("side not Int16");
                    let mut builder = Int8Builder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let v = arr.value(i);
                            let v8 = i8::try_from(v).expect("side out of i8 range");
                            builder.append_value(v8);
                        }
                    }
                    builder.finish()
                }
                arrow::datatypes::DataType::Int32 => {
                    let arr = side_col
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("side not Int32");
                    let mut builder = Int8Builder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let v = arr.value(i);
                            let v8 = i8::try_from(v).expect("side out of i8 range");
                            builder.append_value(v8);
                        }
                    }
                    builder.finish()
                }
                arrow::datatypes::DataType::Int64 => {
                    let arr = side_col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("side not Int64");
                    let mut builder = Int8Builder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let v = arr.value(i);
                            let v8 = i8::try_from(v).expect("side out of i8 range");
                            builder.append_value(v8);
                        }
                    }
                    builder.finish()
                }
                other => panic!("side not Int8/Int16/Int32/Int64 (got {other:?})"),
            }
        };

        let seq = get_i64_opt("seq");
        let ts_local = get_i64_opt("ts_local");

        Self {
            ts_exchange,
            price,
            qty,
            side,
            seq,
            ts_local,
            num_rows,
        }
    }
}

pub struct ArrowTickSource {
    symbol_id: u32,
    reader: ArrowArrayStreamReader,
    current_batch: Option<CachedBatch>,
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
            if let Some(batch) = self
                .current_batch
                .as_ref()
                .filter(|b| self.batch_idx < b.num_rows)
            {
                self.next_tick = Some(self.read_tick_at(batch, self.batch_idx));
                self.batch_idx += 1;
                return;
            }

            // No current batch or batch exhausted, try to load next batch
            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(CachedBatch::new(batch));
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

    fn read_tick_at(&self, batch: &CachedBatch, idx: usize) -> Tick {
        let ts_exchange = batch.ts_exchange.value(idx);
        let price = batch.price.value(idx);
        let qty = batch.qty.value(idx);
        let side_val = batch.side.value(idx);
        let side = Side::try_from(side_val).expect("invalid side");

        let seq = batch
            .seq
            .as_ref()
            .map(|col| col.value(idx) as u64)
            .unwrap_or(0);
        let ts_local = batch
            .ts_local
            .as_ref()
            .map(|col| col.value(idx))
            .unwrap_or(0);

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int8Builder, Int64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi_stream::FFI_ArrowArrayStream;
    use arrow::record_batch::RecordBatchIterator;
    use std::sync::Arc;

    #[test]
    fn test_arrow_tick_source_reads_batch_correctly() {
        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(1000);
        ts_builder.append_value(2000);

        let mut price_builder = Int64Builder::new();
        price_builder.append_value(100);
        price_builder.append_value(101);

        let mut qty_builder = Int64Builder::new();
        qty_builder.append_value(10);
        qty_builder.append_value(20);

        let mut side_builder = Int8Builder::new();
        side_builder.append_value(1);
        side_builder.append_value(-1);

        let ts_array = Arc::new(ts_builder.finish());
        let price_array = Arc::new(price_builder.finish());
        let qty_array = Arc::new(qty_builder.finish());
        let side_array = Arc::new(side_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("side", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![ts_array, price_array, qty_array, side_array],
        )
        .unwrap();

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        let reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        let mut source = ArrowTickSource::new(1, reader);

        // First tick
        let t1 = source.next().expect("tick 1");
        assert_eq!(t1.ts_exchange, 1000);
        assert_eq!(t1.price, 100);
        assert_eq!(t1.qty, 10);
        assert_eq!(t1.side, Side::Buy);

        // Second tick
        let t2 = source.next().expect("tick 2");
        assert_eq!(t2.ts_exchange, 2000);
        assert_eq!(t2.price, 101);
        assert_eq!(t2.qty, 20);
        assert_eq!(t2.side, Side::Sell);

        // EOF
        assert!(source.next().is_none());
    }

    #[test]
    fn test_arrow_tick_source_reads_int64_side() {
        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(1000);
        ts_builder.append_value(2000);

        let mut price_builder = Int64Builder::new();
        price_builder.append_value(100);
        price_builder.append_value(101);

        let mut qty_builder = Int64Builder::new();
        qty_builder.append_value(10);
        qty_builder.append_value(20);

        let mut side_builder = Int64Builder::new();
        side_builder.append_value(1);
        side_builder.append_value(-1);

        let ts_array = Arc::new(ts_builder.finish());
        let price_array = Arc::new(price_builder.finish());
        let qty_array = Arc::new(qty_builder.finish());
        let side_array = Arc::new(side_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("side", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![ts_array, price_array, qty_array, side_array],
        )
        .unwrap();

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        let reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        let mut source = ArrowTickSource::new(1, reader);

        let t1 = source.next().expect("tick 1");
        assert_eq!(t1.side, Side::Buy);

        let t2 = source.next().expect("tick 2");
        assert_eq!(t2.side, Side::Sell);

        assert!(source.next().is_none());
    }
}
