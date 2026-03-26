# Courier — Guvnor Cloud Log Agent

Lightweight log forwarding agent in Rust. Tails files, transforms (filter/parse/mask PII), ships to S3 as compressed Parquet for query-in-place with DuckDB/Athena.

## Build

```bash
cargo build --release
```

## Run

```bash
./target/release/courier --config config/courier.example.yaml
```

## Architecture

```
Sources (file tail) → Transforms (filter/parse/mask) → Buffer (mpsc) → S3 Parquet Sink
```

## Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| timestamp | INT64 | Nanoseconds since epoch |
| message | UTF8 | Log message |
| source | UTF8 | Source name |
| host | UTF8 | Hostname |
| file | UTF8 | Source file path |
| level | UTF8 | Log level |
| fields_json | UTF8 | Structured fields |

Partitioned: `{org_id}/{source}/{date}/{hour}/`
