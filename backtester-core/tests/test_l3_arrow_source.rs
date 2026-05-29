use std::sync::Arc;

use arrow::array::{Int8Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use backtester_core::Side;
use backtester_core::l3_source::{ArrowL3Source, L3Source};
use backtester_core::orderbook_l3::{L3_ADD, L3_DELETE};

fn reader(batch: RecordBatch) -> ArrowArrayStreamReader {
    let schema = batch.schema();
    let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let stream = FFI_ArrowArrayStream::new(Box::new(iter));
    ArrowArrayStreamReader::try_new(stream).expect("reader")
}

#[test]
fn arrow_l3_source_reads_required_order_id_and_action_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("qty", DataType::Int64, false),
        Field::new("side", DataType::Int8, false),
        Field::new("order_id", DataType::UInt64, false),
        Field::new("action", DataType::Int8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_000, 2_000])),
            Arc::new(Int64Array::from(vec![100, 100])),
            Arc::new(Int64Array::from(vec![5, 0])),
            Arc::new(Int8Array::from(vec![1, 1])),
            Arc::new(UInt64Array::from(vec![20, 20])),
            Arc::new(Int8Array::from(vec![L3_ADD as i8, L3_DELETE as i8])),
        ],
    )
    .expect("batch");

    let mut source = ArrowL3Source::try_new(7, reader(batch)).expect("source");
    let first = source.next().expect("next").expect("first update");
    assert_eq!(first.symbol_id, 7);
    assert_eq!(first.order_id, 20);
    assert_eq!(first.side, Side::Buy);
    assert_eq!(first.action, L3_ADD);

    let second = source.next().expect("next").expect("second update");
    assert_eq!(second.action, L3_DELETE);
    assert!(source.next().expect("next").is_none());
}

#[test]
fn arrow_l3_source_missing_order_id_is_a_schema_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("qty", DataType::Int64, false),
        Field::new("side", DataType::Int8, false),
        Field::new("action", DataType::Int8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_000])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int8Array::from(vec![1])),
            Arc::new(Int8Array::from(vec![L3_ADD as i8])),
        ],
    )
    .expect("batch");

    let err = match ArrowL3Source::try_new(7, reader(batch)) {
        Ok(_) => panic!("missing order_id must fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("missing order_id"));
}
