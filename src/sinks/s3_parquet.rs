use crate::config::{OutputFormat, SinkConfig};
use crate::sources::LogEvent;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::info;

pub struct S3ParquetSink {
    config: SinkConfig,
    s3_client: aws_sdk_s3::Client,
    schema: Arc<Schema>,
}

impl S3ParquetSink {
    pub async fn new(config: SinkConfig) -> anyhow::Result<Self> {
        // Always use the standard credential chain (instance role, env vars,
        // IMDS, ~/.aws/credentials). For Guvnor-hosted buckets, the bucket
        // policy additionally validates the guvnor:token object tag as defense
        // in depth. The token-gated approach still works — it just requires
        // the caller to be a valid AWS principal (not truly anonymous).
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new(config.region.clone()))
            .load().await;
        let s3_client = aws_sdk_s3::Client::new(&aws_config);

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
            anonymous = config.write_token.is_some(),
            "S3 sink ready"
        );
        Ok(Self { config, s3_client, schema })
    }

    pub async fn write_batch(&self, events: Vec<LogEvent>) -> anyhow::Result<()> {
        if events.is_empty() { return Ok(()); }

        match self.config.format {
            OutputFormat::Duckdb => self.write_parquet(&events, Compression::ZSTD(Default::default())).await,
            OutputFormat::Athena => self.write_parquet(&events, Compression::SNAPPY).await,
            OutputFormat::Jsonl => self.write_jsonl(&events).await,
        }
    }

    /// Write as Parquet — used by both DuckDB and Athena modes.
    /// DuckDB uses zstd compression, Athena uses snappy.
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

    /// Write as JSON Lines — gzip compressed, one JSON object per log line.
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

        // Gzip compress
        let compressed = {
            use std::io::Write;
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(lines.as_bytes())?;
            encoder.finish()?
        };

        let key = self.build_key(events, "jsonl.gz");
        self.upload(key, compressed, "application/x-ndjson", count).await
    }

    /// Build the S3 key with Hive-style partitions for DuckDB/Athena,
    /// or simple path-based for JSONL.
    fn build_key(&self, events: &[LogEvent], ext: &str) -> String {
        let now = chrono::Utc::now();
        let source = &events[0].source;

        match self.config.format {
            // Hive partition scheme for DuckDB predicate pushdown
            OutputFormat::Duckdb => {
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
            // Athena-compatible Hive partitions (same scheme, Glue crawlers understand it)
            OutputFormat::Athena => {
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
            // Simple path-based for JSONL (human-readable)
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

    /// Upload bytes to S3 with token tagging.
    async fn upload(&self, key: String, body: Vec<u8>, content_type: &str, count: usize) -> anyhow::Result<()> {
        let mut req = self.s3_client.put_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(body))
            .content_type(content_type);

        if let Some(ref token) = self.config.write_token {
            req = req.tagging(format!("guvnor:token={}", token));
        }

        req.send().await?;
        info!(key = %key, events = count, "Batch uploaded");
        Ok(())
    }
}
