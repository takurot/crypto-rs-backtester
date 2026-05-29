use arrow::array::{Array, Int8Array, Int64Array, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::tick_source::TickSourceError;
use crate::types::{L3Update, Side};

pub trait L3Source: Send {
    fn peek(&mut self) -> Result<Option<&L3Update>, TickSourceError>;
    fn next(&mut self) -> Result<Option<L3Update>, TickSourceError>;
    fn symbol_id(&self) -> u32;
}

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
        Ok(Self {
            ts_exchange: get_required_i64(&batch, &["ts_exchange", "ts_event"])?,
            price: get_required_i64(&batch, &["price"])?,
            qty: get_required_i64(&batch, &["qty", "size"])?,
            side: integer_array_to_i8(required_column(&batch, &["side"])?, "side")?,
            order_id: get_required_u64(&batch, &["order_id"])?,
            action: integer_array_to_i8(required_column(&batch, &["action"])?, "action")?,
            num_rows,
        })
    }
}

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
                .filter(|batch| self.batch_idx < batch.num_rows)
            {
                self.next_update = Some(self.read_update_at(batch, self.batch_idx)?);
                self.batch_idx += 1;
                return Ok(());
            }

            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(CachedL3Batch::new(batch)?);
                    self.batch_idx = 0;
                }
                Some(Err(error)) => return Err(TickSourceError::Reader(error)),
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
        let side_value = batch.side.value(idx);
        let side = Side::try_from(side_value)
            .map_err(|_| TickSourceError::InvalidSide { value: side_value })?;

        Ok(L3Update {
            ts_exchange: batch.ts_exchange.value(idx),
            symbol_id: self.symbol_id,
            order_id: batch.order_id.value(idx),
            price: batch.price.value(idx),
            qty: batch.qty.value(idx),
            side,
            action: batch.action.value(idx) as u8,
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
    if !matches!(
        column.data_type(),
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    ) {
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
