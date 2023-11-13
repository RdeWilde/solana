use async_trait::async_trait;
use thiserror::Error;
use crate::bigtable::{RowData, RowKey};

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Failed to initialize cache: {0}")]
    InitializationFailed(String),

    #[error("Failed reading from cache: {0}")]
    CacheReadFailed(String),

    #[error("Failed writing to cache: {0}")]
    CacheWriteFailed(String),
}

impl From<std::io::Error> for CacheError {
    fn from(err: std::io::Error) -> Self {
        Self::InitializationFailed(err.to_string())
    }
}
impl From<CacheError> for std::io::Error {
    fn from(err: CacheError) -> Self {
        Self::new(std::io::ErrorKind::Other, err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CacheError>;


// Create an interface for the cache layer
#[async_trait]
pub trait Cache: Send + Sync + 'static {
    async fn get_row_keys(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
    ) -> Result<Vec<RowKey>>;

    async fn row_key_exists(
        &mut self,
        table_name: &str,
        row_key: RowKey
    ) -> Result<bool>;

    async fn get_single_row_data(
        &mut self,
        table_name: &str,
        row_key: RowKey,
    ) -> Result<RowData>;

    async fn put_row_data(
        &mut self,
        table_name: &str,
        family_name: &str,
        row_data: &[(&RowKey, RowData)],
    ) -> Result<()>;

    async fn get_multi_row_data(
        &mut self,
        table_name: &str,
        row_keys: &[RowKey],
    ) -> Result<Vec<(RowKey, RowData)>>;

    async fn get_row_data(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
    ) -> Result<Vec<(RowKey, RowData)>>;

    async fn get_keys(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        keys_limit: i64
    ) -> Result<Vec<RowKey>>;

    fn box_clone(&self) -> Box<dyn Cache>;
}

impl Clone for Box<dyn Cache> {
    fn clone(&self) -> Box<dyn Cache> {
        self.box_clone()
    }
}
