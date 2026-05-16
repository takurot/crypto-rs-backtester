use crate::types::{Side, Tick};
use crate::utils::prefetch_read_data;
use arrow::array::{Array, Int8Array, Int64Array, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum TickSourceError {
    MissingColumn {
        names: Vec<String>,
    },
    NonIntegerColumn {
        name: String,
        data_type: DataType,
    },
    NullColumn {
        name: String,
    },
    Cast {
        name: String,
        target_type: DataType,
        source: ArrowError,
    },
    OutOfRange {
        name: String,
        target_type: DataType,
    },
    InvalidSide {
        value: i8,
    },
    Reader(ArrowError),
}

impl fmt::Display for TickSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingColumn { names } => write!(f, "missing {}", names.join("/")),
            Self::NonIntegerColumn { name, data_type } => {
                write!(f, "{name} is not an integer column (got {data_type:?})")
            }
            Self::NullColumn { name } => write!(f, "{name} column contains nulls"),
            Self::Cast {
                name,
                target_type,
                source,
            } => write!(f, "{name} cannot be cast to {target_type:?}: {source}"),
            Self::OutOfRange { name, target_type } => {
                write!(f, "{name} contains values out of {target_type:?} range")
            }
            Self::InvalidSide { value } => {
                write!(f, "invalid side {value} (expected 1, -1, or 0)")
            }
            Self::Reader(source) => write!(f, "error reading arrow stream: {source}"),
        }
    }
}

impl Error for TickSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Cast { source, .. } | Self::Reader(source) => Some(source),
            _ => None,
        }
    }
}

/// A source of ticks for a single (exchange, symbol) stream.
///
/// This abstraction allows the engine to be agnostic to whether ticks come from
/// a localized Vec, a CSV file, or an Arrow stream.
pub trait TickSource: Send {
    /// Peek at the next available tick without consuming it.
    /// Returns None if the stream is exhausted.
    fn peek(&mut self) -> Result<Option<&Tick>, TickSourceError>;

    /// Consume the next tick.
    /// Returns None if the stream is exhausted.
    fn next(&mut self) -> Result<Option<Tick>, TickSourceError>;

    /// Returns the symbol_id associated with this source.
    fn symbol_id(&self) -> u32;
}

struct CachedBatch {
    ts_exchange: Int64Array,
    price: Int64Array,
    qty: Int64Array,
    side: Int8Array,
    seq: Option<UInt64Array>,
    ts_local: Option<Int64Array>,
    num_rows: usize,
}

impl CachedBatch {
    fn new(batch: RecordBatch) -> Result<Self, TickSourceError> {
        let num_rows = batch.num_rows();

        let ts_exchange = get_required_i64(&batch, &["ts_exchange", "ts_event"])?;
        let price = get_required_i64(&batch, &["price"])?;
        let qty = get_required_i64(&batch, &["qty", "size"])?;
        let side = integer_array_to_i8(required_column(&batch, &["side"])?, "side")?;
        let seq = get_optional_u64(&batch, "seq")?;
        let ts_local = get_optional_i64(&batch, "ts_local")?;

        Ok(Self {
            ts_exchange,
            price,
            qty,
            side,
            seq,
            ts_local,
            num_rows,
        })
    }
}

fn required_column<'a>(
    batch: &'a RecordBatch,
    names: &[&str],
) -> Result<&'a dyn Array, TickSourceError> {
    names
        .iter()
        .find_map(|name| batch.column_by_name(name))
        .map(|column| column.as_ref())
        .ok_or_else(|| TickSourceError::MissingColumn {
            names: names.iter().map(|name| (*name).to_string()).collect(),
        })
}

fn get_required_i64(batch: &RecordBatch, names: &[&str]) -> Result<Int64Array, TickSourceError> {
    integer_array_to_i64(required_column(batch, names)?, names[0])
}

fn get_optional_i64(
    batch: &RecordBatch,
    name: &str,
) -> Result<Option<Int64Array>, TickSourceError> {
    batch
        .column_by_name(name)
        .map(|column| integer_array_to_i64(column.as_ref(), name))
        .transpose()
}

fn get_optional_u64(
    batch: &RecordBatch,
    name: &str,
) -> Result<Option<UInt64Array>, TickSourceError> {
    batch
        .column_by_name(name)
        .map(|column| integer_array_to_u64(column.as_ref(), name))
        .transpose()
}

fn integer_array_to_i64(column: &dyn Array, name: &str) -> Result<Int64Array, TickSourceError> {
    cast_integer_array(column, name, &DataType::Int64).and_then(|array| {
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .cloned()
            .ok_or_else(|| TickSourceError::Cast {
                name: name.to_string(),
                target_type: DataType::Int64,
                source: ArrowError::CastError("cast returned wrong type".to_string()),
            })
    })
}

fn integer_array_to_u64(column: &dyn Array, name: &str) -> Result<UInt64Array, TickSourceError> {
    cast_integer_array(column, name, &DataType::UInt64).and_then(|array| {
        array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .cloned()
            .ok_or_else(|| TickSourceError::Cast {
                name: name.to_string(),
                target_type: DataType::UInt64,
                source: ArrowError::CastError("cast returned wrong type".to_string()),
            })
    })
}

fn integer_array_to_i8(column: &dyn Array, name: &str) -> Result<Int8Array, TickSourceError> {
    cast_integer_array(column, name, &DataType::Int8).and_then(|array| {
        array
            .as_any()
            .downcast_ref::<Int8Array>()
            .cloned()
            .ok_or_else(|| TickSourceError::Cast {
                name: name.to_string(),
                target_type: DataType::Int8,
                source: ArrowError::CastError("cast returned wrong type".to_string()),
            })
    })
}

fn cast_integer_array(
    column: &dyn Array,
    name: &str,
    target_type: &DataType,
) -> Result<arrow::array::ArrayRef, TickSourceError> {
    if !is_integer_type(column.data_type()) {
        return Err(TickSourceError::NonIntegerColumn {
            name: name.to_string(),
            data_type: column.data_type().clone(),
        });
    }
    if column.null_count() > 0 {
        return Err(TickSourceError::NullColumn {
            name: name.to_string(),
        });
    }

    let casted = cast(column, target_type).map_err(|source| TickSourceError::Cast {
        name: name.to_string(),
        target_type: target_type.clone(),
        source,
    })?;
    if casted.null_count() > 0 {
        return Err(TickSourceError::OutOfRange {
            name: name.to_string(),
            target_type: target_type.clone(),
        });
    }
    Ok(casted)
}

fn is_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

pub struct ArrowTickSource<I> {
    symbol_id: u32,
    reader: I,
    current_batch: Option<CachedBatch>,
    batch_idx: usize,
    next_tick: Option<Tick>,
}

impl<I> ArrowTickSource<I>
where
    I: Iterator<Item = Result<RecordBatch, ArrowError>> + Send,
{
    pub fn try_new(symbol_id: u32, reader: I) -> Result<Self, TickSourceError> {
        let mut source = Self {
            symbol_id,
            reader,
            current_batch: None,
            batch_idx: 0,
            next_tick: None,
        };
        // Pre-load the first tick
        source.advance()?;
        Ok(source)
    }

    fn advance(&mut self) -> Result<(), TickSourceError> {
        loop {
            // If we have a current batch, try to read the next row
            if let Some(batch) = self
                .current_batch
                .as_ref()
                .filter(|b| self.batch_idx < b.num_rows)
            {
                self.next_tick = Some(self.read_tick_at(batch, self.batch_idx)?);
                self.batch_idx += 1;
                return Ok(());
            }

            // No current batch or batch exhausted, try to load next batch
            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(CachedBatch::new(batch)?);
                    self.batch_idx = 0;
                    continue;
                }
                Some(Err(e)) => return Err(TickSourceError::Reader(e)),
                None => {
                    self.current_batch = None;
                    self.next_tick = None;
                    return Ok(());
                }
            }
        }
    }

    #[inline(always)]
    fn read_tick_at(&self, batch: &CachedBatch, idx: usize) -> Result<Tick, TickSourceError> {
        // Prefetch upcoming data (lookahead=2 is a heuristic)
        // Use slice pointer directly to avoid prefetching stack temporaries
        if idx + 2 < batch.num_rows {
            let next = idx + 2;
            // SAFETY: next < num_rows, so the pointer offset is within bounds.
            unsafe {
                prefetch_read_data(batch.ts_exchange.values().as_ptr().add(next));
                prefetch_read_data(batch.price.values().as_ptr().add(next));
                prefetch_read_data(batch.qty.values().as_ptr().add(next));
                prefetch_read_data(batch.side.values().as_ptr().add(next));
            }
        }

        let ts_exchange = batch.ts_exchange.value(idx);
        let price = batch.price.value(idx);
        let qty = batch.qty.value(idx);
        let side_val = batch.side.value(idx);
        let side = Side::try_from(side_val)
            .map_err(|_| TickSourceError::InvalidSide { value: side_val })?;

        let seq = batch.seq.as_ref().map(|col| col.value(idx)).unwrap_or(0);
        let ts_local = batch
            .ts_local
            .as_ref()
            .map(|col| col.value(idx))
            .unwrap_or(0);

        Ok(Tick {
            ts_exchange,
            ts_local,
            seq,
            symbol_id: self.symbol_id,
            price,
            qty,
            side,
            flags: 0,
        })
    }
}

impl<I> TickSource for ArrowTickSource<I>
where
    I: Iterator<Item = Result<RecordBatch, ArrowError>> + Send,
{
    fn peek(&mut self) -> Result<Option<&Tick>, TickSourceError> {
        Ok(self.next_tick.as_ref())
    }

    fn next(&mut self) -> Result<Option<Tick>, TickSourceError> {
        let tick = self.next_tick.take();
        if tick.is_some() {
            self.advance()?;
        }
        Ok(tick)
    }

    fn symbol_id(&self) -> u32 {
        self.symbol_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int8Builder, Int32Builder, Int64Builder, UInt64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
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

        let mut source = ArrowTickSource::try_new(1, reader).expect("source");

        // First tick
        let t1 = source.next().expect("read tick 1").expect("tick 1");
        assert_eq!(t1.ts_exchange, 1000);
        assert_eq!(t1.price, 100);
        assert_eq!(t1.qty, 10);
        assert_eq!(t1.side, Side::Buy);

        // Second tick
        let t2 = source.next().expect("read tick 2").expect("tick 2");
        assert_eq!(t2.ts_exchange, 2000);
        assert_eq!(t2.price, 101);
        assert_eq!(t2.qty, 20);
        assert_eq!(t2.side, Side::Sell);

        // EOF
        assert!(source.next().expect("read eof").is_none());
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

        let mut source = ArrowTickSource::try_new(1, reader).expect("source");

        let t1 = source.next().expect("read tick 1").expect("tick 1");
        assert_eq!(t1.side, Side::Buy);

        let t2 = source.next().expect("read tick 2").expect("tick 2");
        assert_eq!(t2.side, Side::Sell);

        assert!(source.next().expect("read eof").is_none());
    }

    #[test]
    fn test_arrow_tick_source_casts_integer_columns_to_tick_types() {
        let mut ts_builder = Int32Builder::new();
        ts_builder.append_value(1000);
        ts_builder.append_value(2000);

        let mut price_builder = Int32Builder::new();
        price_builder.append_value(100);
        price_builder.append_value(101);

        let mut qty_builder = UInt64Builder::new();
        qty_builder.append_value(10);
        qty_builder.append_value(20);

        let mut side_builder = Int8Builder::new();
        side_builder.append_value(1);
        side_builder.append_value(-1);

        let mut seq_builder = UInt64Builder::new();
        seq_builder.append_value(10);
        seq_builder.append_value(11);

        let ts_array = Arc::new(ts_builder.finish());
        let price_array = Arc::new(price_builder.finish());
        let qty_array = Arc::new(qty_builder.finish());
        let side_array = Arc::new(side_builder.finish());
        let seq_array = Arc::new(seq_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int32, false),
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::UInt64, false),
            Field::new("side", DataType::Int8, false),
            Field::new("seq", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![ts_array, price_array, qty_array, side_array, seq_array],
        )
        .unwrap();

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        let reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        let mut source = ArrowTickSource::try_new(1, reader).expect("source");

        let t1 = source.next().expect("read tick 1").expect("tick 1");
        assert_eq!(t1.ts_exchange, 1000);
        assert_eq!(t1.price, 100);
        assert_eq!(t1.qty, 10);
        assert_eq!(t1.seq, 10);

        let t2 = source.next().expect("read tick 2").expect("tick 2");
        assert_eq!(t2.ts_exchange, 2000);
        assert_eq!(t2.price, 101);
        assert_eq!(t2.qty, 20);
        assert_eq!(t2.seq, 11);

        assert!(source.next().expect("read eof").is_none());
    }

    #[test]
    fn test_arrow_tick_source_missing_required_column_returns_error() {
        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(1000);

        let mut qty_builder = Int64Builder::new();
        qty_builder.append_value(10);

        let mut side_builder = Int8Builder::new();
        side_builder.append_value(1);

        let ts_array = Arc::new(ts_builder.finish());
        let qty_array = Arc::new(qty_builder.finish());
        let side_array = Arc::new(side_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("side", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![ts_array, qty_array, side_array])
            .expect("record batch");

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        let reader = ArrowArrayStreamReader::try_new(stream).expect("reader");

        let err = match ArrowTickSource::try_new(1, reader) {
            Ok(_) => panic!("missing price must error"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("missing price"));
    }

    #[test]
    fn test_arrow_tick_source_invalid_side_returns_error() {
        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(1000);

        let mut price_builder = Int64Builder::new();
        price_builder.append_value(100);

        let mut qty_builder = Int64Builder::new();
        qty_builder.append_value(10);

        let mut side_builder = Int8Builder::new();
        side_builder.append_value(2);

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
        .expect("record batch");

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        let reader = ArrowArrayStreamReader::try_new(stream).expect("reader");

        let err = match ArrowTickSource::try_new(1, reader) {
            Ok(_) => panic!("invalid side must error"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("invalid side"));
    }
}
