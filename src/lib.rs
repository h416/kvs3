pub fn get_bucket(
    bucket_name: &str,
    region_name: &str,
    endpoint: &str,
) -> Result<s3::Bucket, s3::S3Error> {
    let credentials = s3::creds::Credentials::from_env_specific(
        Some("KVS3_ACCESS_KEY"),
        Some("KVS3_SECRET_KEY"),
        None,
        None,
    )?;
    let region: s3::Region = if endpoint.is_empty() {
        region_name.parse()?
    } else {
        s3::Region::Custom {
            region: region_name.into(),
            endpoint: endpoint.into(),
        }
    };

    let bucket = s3::Bucket::new_with_path_style(bucket_name, region, credentials)?;
    Ok(bucket)
}

pub async fn get_keys(bucket: &s3::Bucket, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();

    let result = bucket.list(prefix.to_string(), None).await;
    if result.is_err() {
        return keys;
    }
    let results = result.unwrap();
    for r in results {
        for content in r.contents {
            keys.push(content.key);
        }
    }
    keys
}

pub async fn get(bucket: &s3::Bucket, key: &str) -> (u16, String) {
    let result = bucket.get_object(key).await;
    if result.is_err() {
        return (500, "{}".to_string());
    }
    let (data, code) = result.unwrap();

    let value = String::from_utf8(data).unwrap();
    (code, value)
}

pub async fn set(bucket: &s3::Bucket, key: &str, value: &str, cache_max_age: i64) -> u16 {
    let cache_header = "Cache-Control";
    let mut b = bucket.clone();
    if cache_max_age > 0 {
        let header_value = format!("max-age={}", cache_max_age);
        b.add_header(cache_header, &header_value);
    }

    let data = value.as_bytes();
    let result = b.put_object(key, data).await;

    if result.is_err() {
        return 500;
    }
    let (_, code) = result.unwrap();
    code
}

pub async fn del(bucket: &s3::Bucket, key: &str) -> u16 {
    let result = bucket.delete_object(key).await;
    if result.is_err() {
        return 500;
    }
    let (_, code) = result.unwrap();
    code
}
