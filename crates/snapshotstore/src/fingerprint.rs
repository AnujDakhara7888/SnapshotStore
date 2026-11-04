//! Canonical logical batch fingerprints for replay-safe ingestion.

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};

use crate::errors::TableError;
use crate::metadata::{CanonicalSchemaV1, DataTypeV1, Sha256Digest};
use crate::parquet_io;

const DOMAIN: &[u8] = b"snapshotstore-logical-batch-v1\0";

/// Fingerprints ordered logical rows independently of Arrow batch boundaries
/// and physical Parquet encoding.
///
/// The encoding includes the canonical schema, schema fingerprint, total row
/// count, ordered rows, explicit type/null markers, fixed-width big-endian
/// numbers, and length-prefixed UTF-8 strings. Signed zero is normalized and
/// non-finite floating-point values are rejected.
pub fn fingerprint_batches(
    schema: &CanonicalSchemaV1,
    batches: &[RecordBatch],
) -> Result<Sha256Digest, TableError> {
    let row_count = batches.iter().try_fold(0_u64, |total, batch| {
        total.checked_add(batch.num_rows() as u64)
    });
    let row_count = row_count.ok_or_else(|| TableError::InvalidBatch {
        message: "logical row count overflow".to_owned(),
    })?;
    if row_count == 0 {
        return Err(TableError::EmptyBatch);
    }
    for batch in batches {
        parquet_io::validate_batch_schema(schema, batch)?;
    }

    let mut hasher = Sha256::new();
    hasher.update(DOMAIN);
    hasher.update(schema.fingerprint().as_str().as_bytes());
    hasher.update((schema.fields.len() as u64).to_be_bytes());
    for field in &schema.fields {
        hasher.update((field.name.len() as u64).to_be_bytes());
        hasher.update(field.name.as_bytes());
        hasher.update([type_tag(field.data_type), u8::from(field.nullable)]);
    }
    hasher.update(row_count.to_be_bytes());

    for batch in batches {
        for row in 0..batch.num_rows() {
            for (column_index, field) in schema.fields.iter().enumerate() {
                let column = batch.column(column_index);
                hasher.update([type_tag(field.data_type)]);
                if column.is_null(row) {
                    hasher.update([0]);
                    continue;
                }
                hasher.update([1]);
                match field.data_type {
                    DataTypeV1::Boolean => {
                        let values = downcast::<BooleanArray>(column, "boolean")?;
                        hasher.update([u8::from(values.value(row))]);
                    }
                    DataTypeV1::Int64 => {
                        let values = downcast::<Int64Array>(column, "int64")?;
                        hasher.update(values.value(row).to_be_bytes());
                    }
                    DataTypeV1::UInt64 => {
                        let values = downcast::<UInt64Array>(column, "uint64")?;
                        hasher.update(values.value(row).to_be_bytes());
                    }
                    DataTypeV1::Float64 => {
                        let values = downcast::<Float64Array>(column, "float64")?;
                        let value = values.value(row);
                        if !value.is_finite() {
                            return Err(TableError::InvalidBatch {
                                message: format!(
                                    "non-finite float at logical row {row}, column {column_index}"
                                ),
                            });
                        }
                        let normalized = if value == 0.0 { 0.0 } else { value };
                        hasher.update(normalized.to_bits().to_be_bytes());
                    }
                    DataTypeV1::Utf8 => {
                        let values = downcast::<StringArray>(column, "utf8")?;
                        let value = values.value(row).as_bytes();
                        hasher.update((value.len() as u64).to_be_bytes());
                        hasher.update(value);
                    }
                }
            }
        }
    }

    Ok(Sha256Digest::from_digest_bytes(hasher.finalize().into()))
}

fn type_tag(data_type: DataTypeV1) -> u8 {
    match data_type {
        DataTypeV1::Boolean => 0,
        DataTypeV1::Int64 => 1,
        DataTypeV1::UInt64 => 2,
        DataTypeV1::Float64 => 3,
        DataTypeV1::Utf8 => 4,
    }
}

fn downcast<'a, T: 'static>(
    column: &'a arrow::array::ArrayRef,
    expected: &str,
) -> Result<&'a T, TableError> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| TableError::InvalidBatch {
            message: format!("Arrow column could not be read as {expected}"),
        })
}
