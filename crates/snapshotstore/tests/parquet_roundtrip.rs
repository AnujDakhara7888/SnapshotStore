use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tempfile::tempdir;

fn event_fixture() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![101, 102, 103])) as ArrayRef,
            Arc::new(StringArray::from(vec!["created", "charged", "refunded"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(12.5), None, Some(-4.25)])) as ArrayRef,
        ],
    )
    .expect("the static fixture must match its schema")
}

#[test]
fn arrow_record_batch_round_trips_through_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let expected = event_fixture();
    let directory = tempdir()?;
    let path = directory.path().join("events.parquet");

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, expected.schema(), Some(properties))?;
    writer.write(&expected)?;
    let metadata = writer.close()?;

    assert_eq!(
        metadata.file_metadata().num_rows(),
        expected.num_rows() as i64
    );
    assert!(
        metadata
            .row_groups()
            .iter()
            .flat_map(|row_group| row_group.columns())
            .all(|column| matches!(column.compression(), Compression::ZSTD(_))),
        "every fixture column must use the configured Zstandard codec"
    );

    let file = File::open(&path)?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let actual = reader
        .next()
        .transpose()?
        .expect("the Parquet file must contain the fixture batch");

    assert_eq!(actual, expected);
    assert!(reader.next().is_none(), "the file contains unexpected rows");

    Ok(())
}
