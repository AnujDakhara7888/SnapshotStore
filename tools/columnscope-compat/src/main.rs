use std::env;
use std::fs;

use columnscope::snapshot::SnapshotDescriptor;

fn main() {
    let path = env::args().nth(1).expect("descriptor fixture path");
    let json = fs::read_to_string(path).expect("read descriptor fixture");
    let descriptor = SnapshotDescriptor::from_json(&json)
        .expect("ColumnScope accepts the SnapshotStore descriptor");
    assert_eq!(descriptor.format_version, 1);
    assert_eq!(descriptor.version, 1);
    assert_eq!(descriptor.files.len(), 1);
    println!(
        "ColumnScope accepted descriptor v{} at table version {} with {} file",
        descriptor.format_version,
        descriptor.version,
        descriptor.files.len()
    );
}
