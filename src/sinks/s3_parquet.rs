use crate::config::{OutputFormat, SinkConfig};
use crate::sources::LogEvent;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::{info, error};

pub struct S3ParquetSink {
    config: SinkConfig,
    schema: Arc<Schema>,
    /// For token-gated writes: raw HTTP client (no AWS signature)
    http_client: reqwest::Client,
    /// For BYOB: AWS SDK client with credential chain
    aws_client: Option<aws_sdk_s3::Client>,
}

impl S3ParquetSink {
    pub async fn new(config: SinkConfig) -> anyhow::Result<Self> {
        let http_client = reqwest::Client::new();

        // Only create AWS SDK client if NOT using token-gated writes
        let aws_client = if config.write_token.is_none() {
            let aws_config = aws_config::from_env()
                .region(aws_config::Region::new(config.region.clone()))
                .load().await;
            Some(aws_sdk_s3::Client::new(&aws_config))
        } else {
            None
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("file", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("fields_json", DataType::Utf8, true),
        ]));
        info!(
            bucket = %config.bucket,
            format = ?config.format,
            token_auth = config.write_token.is_some(),
            "S3 sink ready"
        );
        Ok(Self { config, schema, http_client, aws_client })
    }

    pub async fn write_batch(&self, events: Vec<LogEvent>) -> anyhow::Result<()> {
        if events.is_empty() { return Ok(()); }

        match self.config.format {
            OutputFormat::Duckdb => self.write_parquet(&events, Compression::ZSTD(Default::default())).await,
            OutputFormat::Athena => self.write_parquet(&events, Compression::SNAPPY).await,
            OutputFormat::Jsonl => self.write_jsonl(&events).await,
        }
    }

    async fn write_parquet(&self, events: &[LogEvent], compression: Compression) -> anyhow::Result<()> {
        let count = events.len();
        let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp).collect();
        let messages: Vec<&str> = events.iter().map(|e| e.message.as_str()).collect();
        let sources: Vec<&str> = events.iter().map(|e| e.source.as_str()).collect();
        let hosts: Vec<Option<&str>> = events.iter().map(|e| Some(e.host.as_str())).collect();
        let files: Vec<Option<&str>> = events.iter().map(|e| e.file.as_deref()).collect();
        let levels: Vec<Option<&str>> = events.iter().map(|e| e.fields.get("level").map(|s| s.as_str())).collect();
        let fj: Vec<Option<String>> = events.iter().map(|e| {
            if e.fields.is_empty() { None } else { Some(serde_json::to_string(&e.fields).unwrap_or_default()) }
        }).collect();
        let fj_refs: Vec<Option<&str>> = fj.iter().map(|o| o.as_deref()).collect();

        let batch = RecordBatch::try_new(self.schema.clone(), vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(messages)),
            Arc::new(StringArray::from(sources)),
            Arc::new(StringArray::from(hosts)),
            Arc::new(StringArray::from(files)),
            Arc::new(StringArray::from(levels)),
            Arc::new(StringArray::from(fj_refs)),
        ])?;

        let props = WriterProperties::builder().set_compression(compression).build();
        let mut buf = Vec::new();
        { let mut w = ArrowWriter::try_new(&mut buf, self.schema.clone(), Some(props))?; w.write(&batch)?; w.close()?; }

        let key = self.build_key(events, "parquet");
        self.upload(key, buf, "application/vnd.apache.parquet", count).await
    }

    async fn write_jsonl(&self, events: &[LogEvent]) -> anyhow::Result<()> {
        let count = events.len();
        let mut lines = String::with_capacity(count * 256);
        for event in events {
            let obj = serde_json::json!({
                "timestamp": event.timestamp,
                "message": event.message,
                "source": event.source,
                "host": event.host,
                "file": event.file,
                "level": event.fields.get("level"),
                "fields": event.fields,
            });
            lines.push_str(&serde_json::to_string(&obj).unwrap_or_default());
            lines.push('\n');
        }

        let compressed = {
            use std::io::Write;
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(lines.as_bytes())?;
            encoder.finish()?
        };

        let key = self.build_key(events, "jsonl.gz");
        self.upload(key, compressed, "application/x-ndjson", count).await
    }

    fn build_key(&self, events: &[LogEvent], ext: &str) -> String {
        let now = chrono::Utc::now();
        let source = &events[0].source;

        match self.config.format {
            OutputFormat::Duckdb | OutputFormat::Athena => {
                format!(
                    "source={}/year={}/month={}/day={}/hour={}/{}.{}",
                    source,
                    now.format("%Y"),
                    now.format("%m"),
                    now.format("%d"),
                    now.format("%H"),
                    uuid::Uuid::new_v4(),
                    ext,
                )
            }
            OutputFormat::Jsonl => {
                let prefix = self.config.key_prefix
                    .replace("{date}", &now.format("%Y-%m-%d").to_string())
                    .replace("{hour}", &now.format("%H").to_string())
                    .replace("{org_id}", "default")
                    .replace("{source}", source);
                format!("{}{}.{}", prefix, uuid::Uuid::new_v4(), ext)
            }
        }
    }

    /// Upload bytes to S3.
    /// Token-gated: raw unsigned HTTP PUT (no AWS credentials needed).
    /// BYOB: AWS SDK with standard credential chain.
    async fn upload(&self, key: String, body: Vec<u8>, content_type: &str, count: usize) -> anyhow::Result<()> {
        if let Some(ref token) = self.config.write_token {
            // Raw HTTP PUT — no AWS signature, no credentials.
            // S3 bucket policy allows PutObject when the x-amz-tagging header
            // contains the correct guvnor:token value.
            let url = format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.config.bucket, self.config.region, key
            );
            let resp = self.http_client
                .put(&url)
                .header("Content-Type", content_type)
                .header("x-amz-tagging", format!("guvnor:token={}", token))
                .body(body)
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status();
                let err_body = resp.text().await.unwrap_or_default();
                error!(status = %status, body = %err_body, "S3 upload failed");
                anyhow::bail!("S3 upload failed: {} — {}", status, err_body);
            }
        } else if let Some(ref aws_client) = self.aws_client {
            // AWS SDK with credential chain (BYOB buckets)
            aws_client.put_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .body(aws_sdk_s3::primitives::ByteStream::from(body))
                .content_type(content_type)
                .send()
                .await?;
        } else {
            anyhow::bail!("No S3 upload method configured — need write_token or AWS credentials");
        }

        info!(key = %key, events = count, "Batch uploaded");
        Ok(())
    }
}
