use env_logger;
use log::debug;

use warp::{self, http::StatusCode, Filter};

#[macro_use]
extern crate serde_derive;

fn get_token(authorization: &str) -> String {
    let items: Vec<&str> = authorization.split(' ').collect();
    if items.len() == 2 {
        items[1].to_string()
    } else {
        "".to_string()
    }
}

fn filter(val: &serde_json::Value, fields: &[&str]) -> serde_json::Value {
    if val.is_object() {
        let obj = val.as_object().unwrap();
        let mut new_obj = serde_json::map::Map::new();
        for field in fields {
            let k = (*field).to_string();
            if let Some(v) = obj.get(&k) {
                new_obj.insert(k, v.clone());
            }
        }
        serde_json::to_value(new_obj).unwrap()
    } else {
        val.clone()
    }
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct IndexQuery {
    prefix: Option<String>,
    limit: Option<usize>,
    skip: Option<usize>,
    reverse: Option<bool>,
    values: Option<bool>,
    mask: Option<String>,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct GetQuery {
    mask: Option<String>,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct PostQuery {
    cache_max_age: Option<usize>,
}

async fn get_values(
    authorization: String,
    query: IndexQuery,
    bucket: s3::Bucket,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let id_key = get_id(&authorization, "");
    debug!("get_values {}", id_key);

    let prefix = if let Some(prefix) = &query.prefix {
        format!("{}{}", id_key, prefix)
    } else {
        id_key.to_string()
    };

    debug!("prefix: {:?}", &prefix);

    let is_return_value = query.values.is_some() && query.values.unwrap();

    let mut keys = kvs3::get_keys(&bucket, &prefix).await;

    debug!("keys: {:?}", &keys);

    // reverse order
    if query.reverse.is_some() && query.reverse.unwrap() {
        keys.reverse();
    }

    //limit, skip
    let skip = if query.skip.is_some() {
        query.skip.unwrap()
    } else {
        0
    };
    let limit = if query.limit.is_some() {
        query.limit.unwrap()
    } else {
        0
    };
    if skip > 0 || limit > 0 {
        let mut keys2 = Vec::new();
        for key in keys.iter().skip(skip) {
            keys2.push(key.clone());
            if limit > 0 && keys2.len() >= limit {
                break;
            }
        }
        keys = keys2;
    }

    let result = if is_return_value {
        let mut values = Vec::new();

        for k in &keys {
            let local_key = k.replace(&id_key, "");
            debug!("{} {}", k, local_key);

            let (_, v) = kvs3::get(&bucket, &k).await;
            let mut val = to_value(&v);
            if let Some(fields_string) = &query.mask {
                val = filter_value(&val, &fields_string);
            }
            let mut kv = serde_json::map::Map::new();
            kv.insert(local_key.to_string(), val);
            let value = serde_json::to_value(kv).unwrap();
            values.push(value);
        }

        warp::reply::json(&values)
    } else {
        let mut values = Vec::new();
        for k in &keys {
            let local_key = k.replace(&id_key, "");
            values.push(local_key);
        }
        warp::reply::json(&values)
    };

    debug!("get_values {:?} end", &prefix);

    Ok(warp::reply::with_status(result, StatusCode::OK))
}

fn filter_value(val: &serde_json::Value, fields_string: &str) -> serde_json::Value {
    let fields = fields_string.split(',').collect::<Vec<&str>>();

    if val.is_array() {
        let values = val.as_array().unwrap();
        values.iter().map(|x| filter(&x, &fields)).collect()
    } else if val.is_object() {
        filter(&val, &fields)
    } else {
        to_value(&val.to_string())
    }
}

async fn get_value(
    authorization: String,
    oid: String,
    query: GetQuery,
    bucket: s3::Bucket,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let key = get_id(&authorization, &oid);
    debug!("get_value {}", key);

    let (code, value) = kvs3::get(&bucket, &key).await;

    let mut val: serde_json::Value = to_value(&value);

    if let Some(fields_string) = &query.mask {
        val = filter_value(&val, &fields_string);
    }

    debug!("get {:?} {:?} {:?}", &key, code, &value.to_string());

    Ok(warp::reply::with_status(
        warp::reply::json(&val),
        StatusCode::from_u16(code).unwrap(),
    ))
}

async fn delete_key(
    authorization: String,
    oid: String,
    bucket: s3::Bucket,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let key = get_id(&authorization, &oid);
    debug!("delete_key {}", key);

    let code = kvs3::del(&bucket, &key).await;
    Ok(warp::reply::with_status(
        "",
        StatusCode::from_u16(code).unwrap(),
    ))
}

fn to_value(s: &str) -> serde_json::Value {
    // debug!("to_value {}", &s);
    serde_json::from_str(s).unwrap()
}

async fn set_value(
    authorization: String,
    oid: String,
    query: PostQuery,
    value: serde_json::Value,
    bucket: s3::Bucket,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let key = get_id(&authorization, &oid);
    debug!("set_value {}", key);

    let mut cache_max_age_value = 0;
    if let Some(cache_max_age) = query.cache_max_age {
        cache_max_age_value = cache_max_age as i64;
    }

    let value_string = value.to_string();
    debug!(
        "set {:?} {:?} {:?}",
        &key, &value_string, cache_max_age_value
    );
    let code = kvs3::set(&bucket, &key, &value_string, cache_max_age_value).await;

    Ok(warp::reply::with_status(
        "",
        StatusCode::from_u16(code).unwrap(),
    ))
}

fn b3_hash(s: &str) -> String {
    let hash = blake3::hash(s.as_bytes());
    hash.to_hex().as_str().to_string()
}

fn get_id(authorization: &str, object_id: &str) -> String {
    let token = get_token(authorization);
    let auth = token.replace("=", "");
    let hash = b3_hash(&auth);
    let result = format!("{}/{}", hash, object_id);
    result
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    openssl_probe::init_ssl_cert_env_vars();
    env_logger::init();

    let mut args = pico_args::Arguments::from_env();

    let address = args
        .value_from_str(["-a", "--address"])
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string());

    let bucket_name = args
        .value_from_str(["-b", "--bucket"])
        .unwrap_or_else(|_| "bucket".to_string());

    let region = args
        .value_from_str(["-r", "--region"])
        .unwrap_or_else(|_| "us-east-1".to_string());

    let endpoint = args
        .value_from_str(["-e", "--endpoint"])
        .unwrap_or_else(|_| "endpoint".to_string());

    let origin = args
        .value_from_str(["-o", "--origin"])
        .unwrap_or_else(|_| "*".to_string());

    let bucket = kvs3::get_bucket(&bucket_name, &region, &endpoint)?;

    let bucket = warp::any().map(move || bucket.clone());

    let mut cors = warp::cors()
        .allow_headers(vec!["Authorization", "Accept", "Content-Type"])
        .allow_methods(vec!["GET", "POST"]);
    if origin == "*" {
        cors = cors.allow_any_origin();
    } else {
        cors = cors.allow_origin(&origin as &str);
    }

    let authorization = warp::header::<String>("authorization");
    let id_path = warp::path::param::<String>().and(warp::path::end());

    // GET /
    let route1 = warp::get()
        .and(authorization)
        .and(warp::query::<IndexQuery>())
        .and(bucket.clone())
        .and_then(get_values);

    // GET /{key}
    let route2 = warp::get()
        .and(authorization)
        .and(id_path)
        .and(warp::query::<GetQuery>())
        .and(bucket.clone())
        .and_then(get_value);

    // POST /{key}
    let route3 = warp::post()
        .and(authorization)
        .and(id_path)
        .and(warp::query::<PostQuery>())
        .and(warp::body::json())
        .and(bucket.clone())
        .and_then(set_value);

    // DELETE /{key}
    let route4 = warp::delete()
        .and(authorization)
        .and(id_path)
        .and(bucket.clone())
        .and_then(delete_key);

    let api = route2.or(route1).or(route3).or(route4);

    let route = warp::any().and(api).with(cors);

    let socket_address: std::net::SocketAddr = address.parse()?;
    warp::serve(route).run(socket_address).await;

    Ok(())
}
