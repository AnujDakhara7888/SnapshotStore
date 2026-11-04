use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::errors::TableError;
use crate::metadata::{CanonicalSchemaV1, DataTypeV1};
use crate::storage::ObjectKey;

pub(crate) fn arrow_schema(schema: &CanonicalSchemaV1) -> Arc<Schema> {
    Arc::new(Schema::new(
        schema
            .fields
            .iter()
            .map(|field| {
                let data_type = match field.data_type {
                    DataTypeV1::Boolean => DataType::Boolean,
                    DataTypeV1::Int64 => DataType::Int64,
                    DataTypeV1::UInt64 => DataType::UInt64,
                    DataTypeV1::Float64 => DataType::Float64,
                    DataTypeV1::Utf8 => DataType::Utf8,
                };
                Field::new(&field.name, data_type, field.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn validate_batch_schema(
    expected: &CanonicalSchemaV1,
    batch: &RecordBatch,
) -> Result<(), TableError> {
    let expected = arrow_schema(expected);
    if expected.fields() == batch.schema().fields() {
        Ok(())
    } else {
        Err(TableError::SchemaMismatch)
    }
}

pub(crate) fn write_batch(batch: &RecordBatch) -> Result<Bytes, TableError> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("compression level 3 is valid"),
        ))
        .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).map_err(|error| {
            TableError::InvalidBatch {
                message: format!("could not initialize Parquet writer: {error}"),
            }
        })?;
    writer
        .write(batch)
        .map_err(|error| TableError::InvalidBatch {
            message: format!("could not encode Arrow batch: {error}"),
        })?;
    writer
        .into_inner()
        .map(Bytes::from)
        .map_err(|error| TableError::InvalidBatch {
            message: format!("could not finalize Parquet file: {error}"),
        })
}

pub(crate) fn read_batches(
    key: &ObjectKey,
    bytes: Bytes,
    expected: &CanonicalSchemaV1,
) -> Result<Vec<RecordBatch>, TableError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|error| corrupt_data(key, error))?
        .build()
        .map_err(|error| corrupt_data(key, error))?;
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| corrupt_data(key, error))?;
        validate_batch_schema(expected, &batch).map_err(|_| TableError::CorruptData {
            key: key.clone(),
            message: "Parquet schema does not match table schema".to_owned(),
        })?;
        batches.push(batch);
    }
    Ok(batches)
}

fn corrupt_data(key: &ObjectKey, error: impl std::fmt::Display) -> TableError {
    TableError::CorruptData {
        key: key.clone(),
        message: error.to_string(),
    }
}
