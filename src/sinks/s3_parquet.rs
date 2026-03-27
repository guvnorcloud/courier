use crate::config::SinkConfig;
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
        let s3_client = if config.write_token.is_some() {
            // Anonymous-style client for Guvnor-hosted buckets.
            // Uses dummy credentials — actual auth is via the guvnor:token
            // object tag validated by the S3 bucket policy.
            let creds = aws_credential_types::Credentials::new(
                "ANON", "ANON", None, None, "guvnor-token-auth",
            );
            let s3_config = aws_sdk_s3::Config::builder()
                .region(aws_sdk_s3::config::Region::new(config.region.clone()))
                .credentials_provider(creds)
                .behavior_version_latest()
                .build();
            aws_sdk_s3::Client::from_conf(s3_config)
        } else {
            // Standard credential chain (IAM role, env vars, profile)
            // Used for BYOB buckets where the customer provides credentials
            let aws_config = aws_config::from_env()
                .region(aws_config::Region::new(config.region.clone()))
                .load().await;
            aws_sdk_s3::Client::new(&aws_config)
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
            anonymous = config.write_token.is_some(),
            "S3 Parquet sink ready"
        );
        Ok(Self { config, s3_client, schema })
    }

    pub async fn write_batch(&self, events: Vec<LogEvent>) -> anyhow::Result<()> {
        if events.is_empty() { return Ok(()); }
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

        let compression = match self.config.compression.as_str() {
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP(Default::default()),
            _ => Compression::ZSTD(Default::default()),
        };
        let props = WriterProperties::builder().set_compression(compression).build();
        let mut buf = Vec::new();
        { let mut w = ArrowWriter::try_new(&mut buf, self.schema.clone(), Some(props))?; w.write(&batch)?; w.close()?; }

        let now = chrono::Utc::now();
        let key = self.config.key_prefix
            .replace("{date}", &now.format("%Y-%m-%d").to_string())
            .replace("{hour}", &now.format("%H").to_string())
            .replace("{org_id}", "default")
            .replace("{source}", &events[0].source);
        let full_key = format!("{}{}.parquet", key, uuid::Uuid::new_v4());

        let mut req = self.s3_client.put_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(buf))
            .content_type("application/vnd.apache.parquet");

        // Tag with write token for anonymous auth
        if let Some(ref token) = self.config.write_token {
            req = req.tagging(format!("guvnor:token={}", token));
        }

        req.send().await?;

        info!(key = %full_key, events = count, "Batch uploaded");
        Ok(())
    }
}

