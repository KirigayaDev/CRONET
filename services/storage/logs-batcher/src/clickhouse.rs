use crate::{config::Config, error::Error, log::LogRow};
use std::future::Future;

pub trait Sink: Send + Sync {
    fn write(&self, rows: &[LogRow]) -> impl Future<Output = Result<(), Error>> + Send;
}

pub struct ClickHouseSink {
    client: ::clickhouse::Client,
    timeout: std::time::Duration,
}

impl ClickHouseSink {
    pub fn new(config: &Config) -> Self {
        Self {
            client: ::clickhouse::Client::default()
                .with_url(&config.clickhouse_url)
                .with_database(&config.clickhouse_database)
                .with_user(&config.clickhouse_user)
                .with_password(&config.clickhouse_password)
                .with_option("async_insert", "0")
                .with_option("log_comment", format!("logs-batcher:{}", config.instance)),
            timeout: config.insert_timeout,
        }
    }
}

impl Sink for ClickHouseSink {
    async fn write(&self, rows: &[LogRow]) -> Result<(), Error> {
        let write = async {
            let mut insert = self.client.insert("logs")?.with_timeouts(Some(self.timeout), Some(self.timeout));
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        };
        // Driver errors may contain server-provided payloads; keep service logs metadata-only.
        tokio::time::timeout(self.timeout, write).await.map_err(|_| Error::Write)?.map_err(|_| Error::Write)
    }
}
