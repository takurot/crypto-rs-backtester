use arrow::array::{Array, Int8Array, Int64Array, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::tick_source::TickSourceError;
use crate::types::{L3Update, Side, TsExchangeNs};

/// Source of L3 order book events for a single symbol.
pub trait L3Source: Send {
    fn peek(&mut self) -> Result<Option<&L3Update>, TickSourceError>;
    fn next(&mut self) -> Result<Option<L3Update>, TickSourceError>;
    fn symbol_id(&self) -> u32;
}

#[derive(Debug)]
struct CachedL3Batch {
    ts_exchange: Int64Array,
    price: Int64Array,
    qty: Int64Array,
    side: Int8Array,
    order_id: UInt64Array,
    action: Int8Array,
    num_rows: usize,
}

impl CachedL3Batch {
    fn new(batch: RecordBatch) -> Result<Self, TickSourceError> {
        let num_rows = batch.num_rows();
        let ts_exchange = get_required_i64(&batch, &["ts_exchange", "ts_event"])?;
        let price = get_required_i64(&batch, &["price"])?;
        let qty = get_required_i64(&batch, &["qty", "size"])?;
        let side = integer_array_to_i8(required_column(&batch, &["side"])?, "side")?;
        let order_id = get_required_u64(&batch, &["order_id"])?;
        let action = integer_array_to_i8(required_column(&batch, &["action"])?, "action")?;
        Ok(Self {
            ts_exchange,
            price,
            qty,
            side,
            order_id,
            action,
            num_rows,
        })
    }
}

#[derive(Debug)]
pub struct ArrowL3Source<I> {
    symbol_id: u32,
    reader: I,
    current_batch: Option<CachedL3Batch>,
    batch_idx: usize,
    next_update: Option<L3Update>,
}

impl<I> ArrowL3Source<I>
where
    I: Iterator<Item = Result<RecordBatch, ArrowError>> + Send,
{
    pub fn try_new(symbol_id: u32, reader: I) -> Result<Self, TickSourceError> {
        let mut source = Self {
            symbol_id,
            reader,
            current_batch: None,
            batch_idx: 0,
            next_update: None,
        };
        source.advance()?;
        Ok(source)
    }

    fn advance(&mut self) -> Result<(), TickSourceError> {
        loop {
            if let Some(batch) = self
                .current_batch
                .as_ref()
                .filter(|b| self.batch_idx < b.num_rows)
            {
                self.next_update = Some(self.read_update_at(batch, self.batch_idx)?);
                self.batch_idx += 1;
                return Ok(());
            }

            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(CachedL3Batch::new(batch)?);
                    self.batch_idx = 0;
                    continue;
                }
                Some(Err(e)) => return Err(TickSourceError::Reader(e)),
                None => {
                    self.current_batch = None;
                    self.next_update = None;
                    return Ok(());
                }
            }
        }
    }

    fn read_update_at(
        &self,
        batch: &CachedL3Batch,
        idx: usize,
    ) -> Result<L3Update, TickSourceError> {
        let ts_exchange: TsExchangeNs = batch.ts_exchange.value(idx);
        let price = batch.price.value(idx);
        let qty = batch.qty.value(idx);
        let side_val = batch.side.value(idx);
        let side = Side::try_from(side_val)
            .map_err(|_| TickSourceError::InvalidSide { value: side_val })?;
        let order_id = batch.order_id.value(idx);
        let action = batch.action.value(idx) as u8;

        Ok(L3Update {
            ts_exchange,
            order_id,
            price,
            qty,
            symbol_id: self.symbol_id,
            side,
            action,
        })
    }
}

impl<I> L3Source for ArrowL3Source<I>
where
    I: Iterator<Item = Result<RecordBatch, ArrowError>> + Send,
{
    fn peek(&mut self) -> Result<Option<&L3Update>, TickSourceError> {
        Ok(self.next_update.as_ref())
    }

    fn next(&mut self) -> Result<Option<L3Update>, TickSourceError> {
        let update = self.next_update.take();
        if update.is_some() {
            self.advance()?;
        }
        Ok(update)
    }

    fn symbol_id(&self) -> u32 {
        self.symbol_id
    }
}

// ── helper fns (same pattern as tick_source) ────────────────────────────────

fn required_column<'a>(
    batch: &'a RecordBatch,
    names: &[&str],
) -> Result<&'a dyn Array, TickSourceError> {
    names
        .iter()
        .find_map(|name| batch.column_by_name(name))
        .map(|c| c.as_ref())
        .ok_or_else(|| TickSourceError::MissingColumn {
            names: names.iter().map(|n| (*n).to_string()).collect(),
        })
}

fn get_required_i64(batch: &RecordBatch, names: &[&str]) -> Result<Int64Array, TickSourceError> {
    integer_array_to_i64(required_column(batch, names)?, names[0])
}

fn get_required_u64(batch: &RecordBatch, names: &[&str]) -> Result<UInt64Array, TickSourceError> {
    integer_array_to_u64(required_column(batch, names)?, names[0])
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int8Builder, Int64Builder, UInt64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow::record_batch::RecordBatchIterator;
    use std::sync::Arc;

    fn make_l3_stream(
        ts: &[i64],
        price: &[i64],
        qty: &[i64],
        side: &[i8],
        order_id: &[u64],
        action: &[i8],
    ) -> ArrowArrayStreamReader {
        let mut ts_b = Int64Builder::new();
        let mut price_b = Int64Builder::new();
        let mut qty_b = Int64Builder::new();
        let mut side_b = Int8Builder::new();
        let mut oid_b = UInt64Builder::new();
        let mut act_b = Int8Builder::new();

        for &v in ts {
            ts_b.append_value(v);
        }
        for &v in price {
            price_b.append_value(v);
        }
        for &v in qty {
            qty_b.append_value(v);
        }
        for &v in side {
            side_b.append_value(v);
        }
        for &v in order_id {
            oid_b.append_value(v);
        }
        for &v in action {
            act_b.append_value(v);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("side", DataType::Int8, false),
            Field::new("order_id", DataType::UInt64, false),
            Field::new("action", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts_b.finish()),
                Arc::new(price_b.finish()),
                Arc::new(qty_b.finish()),
                Arc::new(side_b.finish()),
                Arc::new(oid_b.finish()),
                Arc::new(act_b.finish()),
            ],
        )
        .unwrap();

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        ArrowArrayStreamReader::try_new(stream).unwrap()
    }

    #[test]
    fn test_arrow_l3_source_reads_batch_correctly() {
        let reader = make_l3_stream(
            &[1000, 2000],
            &[100, 100],
            &[10, 5],
            &[1, 1],
            &[42, 43],
            &[1, 3], // ADD, DELETE
        );

        let mut source = ArrowL3Source::try_new(1, reader).expect("source");

        let u1 = source.next().unwrap().unwrap();
        assert_eq!(u1.ts_exchange, 1000);
        assert_eq!(u1.order_id, 42);
        assert_eq!(u1.qty, 10);
        assert_eq!(u1.side, Side::Buy);
        assert_eq!(u1.action, 1); // ADD

        let u2 = source.next().unwrap().unwrap();
        assert_eq!(u2.order_id, 43);
        assert_eq!(u2.action, 3); // DELETE

        assert!(source.next().unwrap().is_none());
    }

    #[test]
    fn test_arrow_l3_source_missing_order_id_returns_error() {
        let mut ts_b = Int64Builder::new();
        ts_b.append_value(1000);
        let mut price_b = Int64Builder::new();
        price_b.append_value(100);
        let mut qty_b = Int64Builder::new();
        qty_b.append_value(10);
        let mut side_b = Int8Builder::new();
        side_b.append_value(1);
        let mut act_b = Int8Builder::new();
        act_b.append_value(1);

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("side", DataType::Int8, false),
            Field::new("action", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts_b.finish()),
                Arc::new(price_b.finish()),
                Arc::new(qty_b.finish()),
                Arc::new(side_b.finish()),
                Arc::new(act_b.finish()),
            ],
        )
        .unwrap();

        let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter));
        let reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        let err = ArrowL3Source::try_new(1, reader).unwrap_err();
        assert!(err.to_string().contains("missing order_id"));
    }
}
