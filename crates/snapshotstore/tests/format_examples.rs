use snapshotstore::errors::MetadataError;
use snapshotstore::metadata::{Head, ManifestV1, TableMetadata, validate_snapshot};

const TABLE: &str = include_str!("../../../fixtures/format-v1/table.json");
const HEAD: &str = include_str!("../../../fixtures/format-v1/head.json");
const MANIFEST_V0: &str = include_str!("../../../fixtures/format-v1/manifest-v0.json");
const MANIFEST_V1: &str = include_str!("../../../fixtures/format-v1/manifest-v1.json");

fn parse_examples() -> (TableMetadata, Head, ManifestV1, ManifestV1) {
    (
        serde_json::from_str(TABLE).expect("table example must decode"),
        serde_json::from_str(HEAD).expect("HEAD example must decode"),
        serde_json::from_str(MANIFEST_V0).expect("version-0 manifest must decode"),
        serde_json::from_str(MANIFEST_V1).expect("version-1 manifest must decode"),
    )
}

#[test]
fn checked_in_examples_round_trip_without_field_loss() {
    let (table, head, manifest_v0, manifest_v1) = parse_examples();

    for (source, encoded) in [
        (TABLE, serde_json::to_value(&table).expect("table encodes")),
        (HEAD, serde_json::to_value(&head).expect("HEAD encodes")),
        (
            MANIFEST_V0,
            serde_json::to_value(&manifest_v0).expect("version-0 manifest encodes"),
        ),
        (
            MANIFEST_V1,
            serde_json::to_value(&manifest_v1).expect("version-1 manifest encodes"),
        ),
    ] {
        let expected: serde_json::Value = serde_json::from_str(source).expect("valid JSON");
        assert_eq!(encoded, expected);
    }
}

#[test]
fn examples_satisfy_internal_and_cross_object_invariants() {
    let (table, head, manifest_v0, manifest_v1) = parse_examples();

    table.validate().expect("table is valid");
    manifest_v0.validate().expect("version 0 is valid");
    validate_snapshot(&table, &head, &manifest_v1).expect("version 1 snapshot is valid");
}

#[test]
fn unsupported_format_is_explicit() {
    let (mut table, _, _, _) = parse_examples();
    table.format_version = 2;
    assert_eq!(
        table.validate(),
        Err(MetadataError::UnsupportedFormat { found: 2 })
    );
}

#[test]
fn corrupt_row_count_is_not_silently_accepted() {
    let (_, _, _, mut manifest) = parse_examples();
    manifest.row_count += 1;
    assert!(matches!(
        manifest.validate(),
        Err(MetadataError::CorruptMetadata { .. })
    ));
}

#[test]
fn duplicate_files_are_rejected() {
    let (_, _, _, mut manifest) = parse_examples();
    manifest.files.push(manifest.files[0].clone());
    manifest.row_count *= 2;
    assert!(matches!(
        manifest.validate(),
        Err(MetadataError::CorruptMetadata { .. })
    ));
}

#[test]
fn mismatched_head_commit_is_rejected() {
    let (table, mut head, _, manifest) = parse_examples();
    head.version = 2;
    assert!(matches!(
        validate_snapshot(&table, &head, &manifest),
        Err(MetadataError::CorruptMetadata { .. })
    ));
}

#[test]
fn linked_identity_mismatches_return_typed_errors() {
    let (table, mut head, _, manifest) = parse_examples();
    head.table_id =
        uuid::Uuid::parse_str("88888888-8888-4888-8888-888888888888").expect("valid UUID");
    assert!(matches!(
        validate_snapshot(&table, &head, &manifest),
        Err(MetadataError::TableMismatch { .. })
    ));

    let (mut table, head, _, manifest) = parse_examples();
    table.schema_fingerprint = snapshotstore::metadata::Sha256Digest::parse(
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    )
    .expect("valid digest");
    assert_eq!(
        validate_snapshot(&table, &head, &manifest),
        Err(MetadataError::SchemaMismatch)
    );

    let (table, mut head, _, manifest) = parse_examples();
    head.manifest_key = "manifests/99999999-9999-4999-8999-999999999999.json"
        .parse()
        .expect("valid manifest key");
    assert!(matches!(
        validate_snapshot(&table, &head, &manifest),
        Err(MetadataError::ManifestKeyMismatch { .. })
    ));
}

#[test]
fn duplicate_schema_fields_are_rejected() {
    let (mut table, _, _, _) = parse_examples();
    table.schema.fields.push(table.schema.fields[0].clone());
    assert!(matches!(
        table.validate(),
        Err(MetadataError::CorruptMetadata { .. })
    ));
}

#[test]
fn malformed_digest_and_object_key_fail_during_decode() {
    let bad_digest = TABLE.replace(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "ABC",
    );
    assert!(serde_json::from_str::<TableMetadata>(&bad_digest).is_err());

    let bad_key = HEAD.replace(
        "manifests/33333333-3333-4333-8333-333333333333.json",
        "../manifest.json",
    );
    assert!(serde_json::from_str::<Head>(&bad_key).is_err());
}

#[test]
fn data_file_shape_and_attempt_identity_are_enforced() {
    let (_, _, _, mut manifest) = parse_examples();
    manifest.files[0].object_key = "files/not-the-layout.parquet"
        .parse()
        .expect("relative key");
    assert!(matches!(
        manifest.validate(),
        Err(MetadataError::CorruptMetadata { .. })
    ));

    let (_, _, _, mut manifest) = parse_examples();
    manifest.operation.attempt_id =
        uuid::Uuid::parse_str("77777777-7777-4777-8777-777777777777").expect("valid UUID");
    assert!(matches!(
        manifest.validate(),
        Err(MetadataError::CorruptMetadata { .. })
    ));
}
