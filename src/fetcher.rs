use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::fs;

#[async_trait]
trait Fetch {
    type Error;
    async fn fetch(&self) -> Result<String, Self::Error>;
}

struct URLFetcher(String);

struct FileFetcher(String);

#[async_trait]
impl Fetch for URLFetcher {
    type Error = anyhow::Error;
    async fn fetch(&self) -> Result<String, Self::Error> {
        Ok(reqwest::get(self.0.as_str()).await?.text().await?)
    }
}

#[async_trait]
impl Fetch for FileFetcher {
    type Error = anyhow::Error;
    async fn fetch(&self) -> Result<String, Self::Error> {
        Ok(fs::read_to_string(&self.0[7..]).await?)
    }
}

pub async fn fetch(source: String) -> Result<String> {
    match &source[..4] {
        "http" => URLFetcher(source.clone()).fetch().await,
        "file" => FileFetcher(source.clone()).fetch().await,
        _ => Err(anyhow!("Only support http/https/file right now")),
    }
}