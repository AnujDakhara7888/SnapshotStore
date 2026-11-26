use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use clap::Parser;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig, UpdateVersion,
};
use serde::Serialize;
use uuid::Uuid;

type CheckResult<T> = Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[command(about = "Check conditional writes in an authorized AWS S3 test prefix")]
struct Args {
    /// Disposable s3://bucket/prefix (defaults to SNAPSHOTSTORE_TEST_PREFIX).
    #[arg(long)]
    prefix: Option<String>,

    /// Write a new JSON report after all four checks pass.
    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Serialize)]
struct Report<'a> {
    backend: &'a str,
    client: &'a str,
    conditional_put: &'a str,
    source_sha: &'a str,
    worktree_clean: bool,
    object_suffix: &'a str,
    checks_passed: [&'a str; 4],
}

fn parse_prefix(value: &str) -> CheckResult<(&str, &str)> {
    let (bucket, prefix) = value
        .strip_prefix("s3://")
        .and_then(|value| value.split_once('/'))
        .ok_or("expected an authorized s3://bucket/prefix")?;
    let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
    let valid_segment = |segment: &str| {
        !matches!(segment, "" | "." | "..")
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    };
    if !(3..=63).contains(&bucket.len())
        || !bucket.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
        })
        || !bucket.starts_with(|ch: char| ch.is_ascii_alphanumeric())
        || !bucket.ends_with(|ch: char| ch.is_ascii_alphanumeric())
        || prefix.len() > 900
        || !prefix.split('/').all(valid_segment)
    {
        return Err("expected an authorized s3://bucket/prefix with plain key segments".into());
    }
    Ok((bucket, prefix))
}

fn credentials_var(name: &str) -> CheckResult<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("set {name} in the environment before running the AWS check").into())
}

fn source_state() -> CheckResult<(String, bool)> {
    let git = |args: &[&str]| {
        Command::new("git")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(args)
            .output()
    };
    let head = git(&["rev-parse", "HEAD"])?;
    let status = git(&["status", "--porcelain"])?;
    if !head.status.success() || !status.status.success() {
        return Err("could not identify the source checkout".into());
    }
    let sha = String::from_utf8(head.stdout)?.trim().to_owned();
    Ok((sha, status.stdout.is_empty()))
}

// Report only the operation and error category. Cloud errors can carry request
// details and the bucket or key used for the test.
fn store_error(operation: &str, error: object_store::Error) -> Box<dyn Error> {
    use object_store::Error::*;
    let category = match error {
        NotFound { .. } => "object missing",
        PermissionDenied { .. } => "permission denied",
        Unauthenticated { .. } => "authentication failed",
        AlreadyExists { .. } => "object already exists or request conflicted",
        Precondition { .. } => "precondition rejected",
        NotImplemented { .. } | NotSupported { .. } => "operation unsupported",
        _ => "client or service error",
    };
    format!("{operation} failed ({category})").into()
}

async fn read_etag(
    client: &impl ObjectStore,
    key: &Path,
    expected_bytes: &[u8],
) -> CheckResult<String> {
    let result = client
        .get(key)
        .await
        .map_err(|error| store_error("GET test object", error))?;
    let token = result
        .meta
        .e_tag
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or("GET test object returned no ETag")?;
    let bytes = result
        .bytes()
        .await
        .map_err(|error| store_error("read test object body", error))?;
    if bytes != expected_bytes {
        return Err("GET test object returned unexpected contents".into());
    }
    Ok(token)
}

fn update_options(etag: &str) -> PutOptions {
    PutMode::Update(UpdateVersion {
        e_tag: Some(etag.to_owned()),
        version: None,
    })
    .into()
}

async fn check_conditional_writes(
    first: &impl ObjectStore,
    second: &impl ObjectStore,
    key: &Path,
) -> CheckResult<()> {
    first
        .put_opts(key, "initial".into(), PutMode::Create.into())
        .await
        .map_err(|error| store_error("create absent object", error))?;
    match second
        .put_opts(key, "duplicate".into(), PutMode::Create.into())
        .await
    {
        Err(object_store::Error::AlreadyExists { .. }) => {}
        Err(error) => return Err(store_error("reject duplicate create", error)),
        Ok(_) => return Err("duplicate create unexpectedly succeeded".into()),
    }
    let initial = read_etag(first, key, b"initial").await?;

    first
        .put_opts(key, "updated".into(), update_options(&initial))
        .await
        .map_err(|error| store_error("replace with current ETag", error))?;
    let current = read_etag(second, key, b"updated").await?;
    if current == initial {
        return Err("different test contents returned the same ETag".into());
    }
    match first
        .put_opts(key, "stale".into(), update_options(&initial))
        .await
    {
        Err(object_store::Error::Precondition { .. }) => {}
        Err(error) => return Err(store_error("reject stale ETag", error)),
        Ok(_) => return Err("replacement with stale ETag unexpectedly succeeded".into()),
    }
    let same_base = read_etag(first, key, b"updated").await?;
    if same_base != current {
        return Err("stale replacement changed the current ETag".into());
    }

    let (left, right) = tokio::join!(
        first.put_opts(key, "left".into(), update_options(&same_base)),
        second.put_opts(key, "right".into(), update_options(&current))
    );
    let winner: &[u8] = match (left, right) {
        (Ok(_), Err(object_store::Error::Precondition { .. })) => b"left",
        (Err(object_store::Error::Precondition { .. }), Ok(_)) => b"right",
        (Err(error), _) | (_, Err(error)) => return Err(store_error("same-ETag race", error)),
        (Ok(_), Ok(_)) => return Err("both same-ETag writers unexpectedly succeeded".into()),
    };
    read_etag(first, key, winner).await?;
    Ok(())
}

async fn run(args: Args) -> CheckResult<()> {
    let prefix = args
        .prefix
        .or_else(|| env::var("SNAPSHOTSTORE_TEST_PREFIX").ok())
        .filter(|value| !value.is_empty())
        .ok_or("set SNAPSHOTSTORE_TEST_PREFIX or pass --prefix with an authorized S3 path")?;
    let (bucket, prefix) = parse_prefix(&prefix)?;
    let access_key_id = credentials_var("AWS_ACCESS_KEY_ID")?;
    let secret_access_key = credentials_var("AWS_SECRET_ACCESS_KEY")?;
    let region =
        credentials_var("AWS_REGION").or_else(|_| credentials_var("AWS_DEFAULT_REGION"))?;
    if !region
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("AWS region must be a plain region name".into());
    }
    let (source_sha, worktree_clean) = source_state()?;
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key_id)
        .with_secret_access_key(secret_access_key)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(ClientOptions::new().with_timeout(Duration::from_secs(15)))
        .with_retry(RetryConfig {
            max_retries: 2,
            retry_timeout: Duration::from_secs(30),
            ..Default::default()
        });
    if let Ok(token) = env::var("AWS_SESSION_TOKEN")
        && !token.is_empty()
    {
        builder = builder.with_token(token);
    }
    let first = builder
        .clone()
        .build()
        .map_err(|error| store_error("build first AWS client", error))?;
    let second = builder
        .build()
        .map_err(|error| store_error("build second AWS client", error))?;
    let suffix = format!("snapshotstore-cas-{}", Uuid::new_v4());
    let key = Path::parse(format!("{prefix}/{suffix}"))?;
    // Print the suffix before writing so the operator can find the fixture
    // even if a request times out or a check fails.
    eprintln!("AWS S3 test object suffix: {suffix}");
    check_conditional_writes(&first, &second, &key).await?;
    let report = Report {
        backend: "AWS S3",
        client: "object_store 0.14.2",
        conditional_put: "ETagMatch",
        source_sha: &source_sha,
        worktree_clean,
        object_suffix: &suffix,
        checks_passed: [
            "create_if_absent",
            "replace_with_current_etag",
            "reject_stale_etag",
            "race_same_etag",
        ],
    };
    let mut json = serde_json::to_vec_pretty(&report)?;
    json.push(b'\n');
    use std::io::Write;
    if let Some(path) = args.report {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?
            .write_all(&json)?;
    }
    std::io::stdout().lock().write_all(&json)?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match run(Args::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("S3 conditional-write check did not pass: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use http_body_util::BodyExt;
    use object_store::HeaderValue;
    use object_store::client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
    };

    use super::*;

    #[derive(Debug, Default)]
    struct Fixture {
        object: Option<(String, Bytes)>,
        writes: usize,
    }

    #[derive(Debug, Clone, Default)]
    struct FixtureConnector(Arc<Mutex<Fixture>>);

    impl HttpConnector for FixtureConnector {
        fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    #[async_trait::async_trait]
    impl HttpService for FixtureConnector {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            assert!(request.uri().path().ends_with("/disposable/cas-check"));
            let (request, body) = request.into_parts();
            let bytes = body.collect().await?.to_bytes();
            let mut fixture = self.0.lock().unwrap();
            if request.method == "GET" {
                let (tag, bytes) = fixture.object.as_ref().expect("test object exists");
                let mut response = HttpResponse::new(bytes.clone().into());
                let headers = response.headers_mut();
                headers.insert("etag", tag.parse().unwrap());
                headers.insert("content-length", bytes.len().to_string().parse().unwrap());
                headers.insert(
                    "last-modified",
                    HeaderValue::from_static("Wed, 25 Nov 2026 15:00:00 GMT"),
                );
                return Ok(response);
            }

            assert_eq!(request.method, "PUT");
            let matches = request.headers.get("if-match");
            let absent = request.headers.get("if-none-match");
            let allowed = match (absent, matches) {
                (Some(wildcard), None) => {
                    assert_eq!(wildcard, "*");
                    fixture.object.is_none()
                }
                (None, Some(token)) => fixture
                    .object
                    .as_ref()
                    .is_some_and(|(tag, _)| token == tag.as_str()),
                _ => panic!("expected exactly one conditional write header"),
            };
            let mut response = HttpResponse::new(Bytes::new().into());
            if allowed {
                fixture.writes += 1;
                let tag = format!("\"fixture-{}\"", fixture.writes);
                response.headers_mut().insert("etag", tag.parse().unwrap());
                fixture.object = Some((tag, bytes));
            } else {
                *response.status_mut() = 412.try_into().unwrap();
            }
            Ok(response)
        }
    }

    #[test]
    fn requires_a_disposable_prefix() {
        assert_eq!(
            parse_prefix("s3://test-bucket/team/snapshotstore/").unwrap(),
            ("test-bucket", "team/snapshotstore")
        );
        for invalid in [
            "s3://test-bucket",
            "s3://test-bucket/",
            "s3://test-bucket/a/../b",
            "s3://test-bucket/a//b",
            "s3://test-bucket/a/%2e%2e",
            "https://test-bucket/key",
        ] {
            assert!(parse_prefix(invalid).is_err());
        }
    }

    #[tokio::test]
    async fn s3_client_sends_conditional_headers_and_checks_the_winner() {
        let connector = FixtureConnector::default();
        let builder = AmazonS3Builder::new()
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-key")
            .with_secret_access_key("test-secret")
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_http_connector(connector.clone());
        let first = builder.clone().build().unwrap();
        let second = builder.build().unwrap();
        check_conditional_writes(&first, &second, &Path::from("disposable/cas-check"))
            .await
            .unwrap();
        assert_eq!(connector.0.lock().unwrap().writes, 3);
    }
}
