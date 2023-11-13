use async_trait::async_trait;
use aws_sdk_s3::client::Client;
use aws_sdk_s3::config::{Config, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use log::{info};
use crate::bigtable::{RowData, RowKey};
use crate::cache::cache::{Cache, CacheError};
use crate::cache::cache::CacheError::*;

#[derive(Clone)]
pub struct S3Cache {
    access_key: String,
    secret_key: String,
    endpoint: String,
    bucket: String,
    region: Option<String>,
    provider_name: Option<String>,
    prefix: String,
    client: Client,
}

impl S3Cache {
    pub async fn new(
        access_key: String,
        secret_key: String,
        endpoint: String,
        bucket: String,
        region: String,
        prefix: String,
    ) -> Result<Option<Self>, CacheError> {
        static PROVIDER_NAME: &str = "Wasabi"; // FIXME

        if access_key.is_empty() || secret_key.is_empty() || endpoint.is_empty() || bucket.is_empty() || region.is_empty() || prefix.is_empty() {
            info!("Not using S3 cache: access_key={}, secret_key={}, endpoint={}, region={}, bucket={}, prefix={}", access_key, "<redacted>", endpoint, region, bucket, prefix);

            if access_key.is_empty() {
                InitializationFailed("S3_ACCESS_KEY is empty".to_string());
            }
            if secret_key.is_empty() {
                InitializationFailed("S3_SECRET_KEY is empty".to_string());
            }
            if endpoint.is_empty() {
                InitializationFailed("S3_ENDPOINT is empty".to_string());
            }
            if bucket.is_empty() {
                InitializationFailed("S3_BUCKET is empty".to_string());
            }
            if region.is_empty() {
                InitializationFailed("S3_REGION is empty".to_string());
            }
            if prefix.is_empty() {
                InitializationFailed("S3_PREFIX is empty".to_string());
            }

            InitializationFailed("S3 cache not initialized".to_string());
        }

        let creds = Credentials::new(access_key.clone(), secret_key.clone(), None, None, PROVIDER_NAME);

        let config = Config::builder()
            // .disable_multi_region_access_points(true)
            .region(Region::new(region.clone()))
            .endpoint_url(endpoint.clone())
            .credentials_provider(creds)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(config);

        Ok(Some(Self {
            access_key,
            secret_key,
            endpoint,
            bucket,
            region: Some(region),
            provider_name: Some(PROVIDER_NAME.to_string()),
            prefix,
            client,
        }))
    }
}

#[async_trait]
impl Cache for S3Cache {
    async fn get_row_keys(&mut self, table_name: &str, start_at: Option<RowKey>, end_at: Option<RowKey>, rows_limit: i64) -> Result<Vec<RowKey>, CacheError> {
        return self.get_keys(table_name, start_at, end_at, rows_limit).await
    }

    async fn row_key_exists(&mut self, table_name: &str, row_key: RowKey) -> Result<bool, CacheError> {
        let key = format!("{}/{}/{}", self.prefix, table_name, row_key);

        let storage_key = self.get_keys(table_name, Some(key), None, 1).await?;

        info!("Checked if row exists in cache");

        return Ok(storage_key.len() > 0 && storage_key[0] == row_key);
    }

    async fn get_single_row_data(
        &mut self,
        table_name: &str,
        row_key: RowKey,
    ) -> Result<RowData, CacheError> {
        let key = format!("{}/{}/{}", self.prefix, table_name, row_key);

        let obj = self.client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key.clone())
            .send()
            .await
            .map_err(|err| {
                return CacheReadFailed(format!("Object not found {} with {}", key, err.to_string()));
            });

        if obj.is_err() {
            return Err(CacheReadFailed(format!("Object not found {}", key)))
        }

        // Turn obj into Vec<u8>
        let body = obj.unwrap().body;

        let bytes = body.collect()
            .await
            .map_err(|err| {
                return CacheReadFailed(format!("Error collecting ByteStream with {}", err.to_string()));
            }).unwrap();

        let mut row_data = RowData::new();
        row_data.push(("proto".parse().unwrap(), bytes.to_vec())); // TODO use correct serialization

        info!("Fetched from cache");

        return Ok(row_data); // TODO
    }

    async fn put_row_data(&mut self, table_name: &str, family_name: &str, row_data: &[(&RowKey, RowData)]) -> Result<(), CacheError> {
        for (key, data) in row_data {
            let full_key = format!("{}/{}/{}", self.prefix, table_name, key);

            let mut obj = self.client
                .put_object()
                .bucket(self.bucket.clone())
                .key(full_key.clone())
                .metadata("family", family_name);

            // Convert row_data into bytec
            let mut input = Vec::new();
            for (column_key, column_value) in data {
                obj = obj.metadata("encoding", column_key.clone());
                input.extend_from_slice(column_value);
            }
            let body = ByteStream::from(input);
            let _ = obj.body(body)
                .send()
                .await
                .map_err(|err| {
                    return CacheWriteFailed(format!("Error while writing to cache {}: {}", full_key.clone(), err.to_string()));
                });

            info!("Written to cache {}", full_key);

            return Ok(());
        }

        Err(CacheWriteFailed("Could not write row to cache".parse().unwrap()))
    }

    async fn get_multi_row_data(&mut self, table_name: &str, row_keys: &[RowKey]) -> Result<Vec<(RowKey, RowData)>, CacheError> {
        let mut results = Vec::new();

        for row_key in row_keys {
            let row_data = self.get_single_row_data(table_name, row_key.clone()).await;

            match row_data {
                Ok(row_data) => {
                    info!("Fetched rom cache");
                    results.push((row_key.clone(), row_data));
                },
                Err(err) => return Err(CacheReadFailed(format!("get_multi_row_data failed with {}", err.to_string()))) // FIXME fallback to bigtable
            }
        }

        return Ok(results);
    }

    async fn get_row_data(&mut self, table_name: &str, start_at: Option<RowKey>, end_at: Option<RowKey>, rows_limit: i64) -> Result<Vec<(RowKey, RowData)>, CacheError> {
        let mut results = Vec::new();

        let keys = self.get_keys(table_name, start_at, end_at, rows_limit).await?;

        for row_key in keys {
            let row_data = self.get_single_row_data(table_name, row_key.clone()).await;

            match row_data {
                Ok(row_data) => {
                    info!("Fetched rom cache");
                    results.push((row_key.clone(), row_data));
                },
                Err(err) => return Err(CacheReadFailed(format!("Error getting row data with {}", err.to_string())))
            }
        }

        return Ok(results);
    }

    async fn get_keys(&mut self, table_name: &str, start_at: Option<RowKey>, end_at: Option<RowKey>, keys_limit: i64) -> Result<Vec<RowKey>, CacheError> {
        let mut keys = vec![];
        let start_key = start_at.unwrap_or("".to_string());
        let end_key = end_at.unwrap_or("".to_string());
        let prefix = format!("{}/{}", self.prefix, table_name);

        // Nasty hack as start_after does not include the key itself, we should do the last char minus 1
        let before_start_key = previous_alphanumeric(start_key.clone());

        let full_start_key = format!("{}/{}", prefix.clone(), before_start_key);

        let storage_keys_result = self.client.list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .start_after(full_start_key)
            .max_keys(keys_limit as i32)
            .send()
            .await;

        let storage_keys = match storage_keys_result {
            Ok(keys) => keys,
            Err(err) => return Err(CacheReadFailed(format!("Could not read {}", err.to_string())))
        };

        if storage_keys.contents().is_some() {
            for obj in storage_keys.contents().unwrap() {
                // If key string equals end_at, break
                if obj.key().unwrap() == end_key.to_string() {
                    break;
                }
                if keys.len() >= keys_limit as usize {
                    break;
                }

                let key_string = obj.key().unwrap();
                // Left trim prefix from key_string
                let key_string_trimmed = key_string.trim_start_matches(&prefix).trim_start_matches("/");
                keys.push(key_string_trimmed.to_string());
            }
        }

        info!("Fetched {} keys from cache", keys.len());

        Ok(keys)
    }

    fn box_clone(&self) -> Box<dyn Cache> {
        Box::new(self.clone())
    }
}


fn previous_alphanumeric(input: String) -> String {
    // If input is only zeros, return empty string
    if input.chars().all(|c| c == '0') {
        return "".to_string();
    }

    let mut result: Vec<char> = input.chars().collect();
    for i in (0..result.len()).rev() {
        if result[i].is_ascii_alphabetic() {
            if result[i] > 'a' {
                result[i] = (result[i] as u8 - 1) as char;
                break;
            } else {
                result[i] = if result[i].is_ascii_lowercase() {
                    'z'
                } else {
                    '9'
                };
            }
        }
    }
    result.iter().collect()
}