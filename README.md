# kvs3

key value json store using aws s3.

---

run

local minio example
```
KVS3_ACCESS_KEY=xxxx KVS3_SECRET_KEY=xxxx ./target/release/kvs3 -b mybucket -e http://127.0.0.1:9000
```

set 

```
curl -H "Authorization: Basic user1" -H "Content-type: application/json" -X POST -d '{"first":"Ada","last":"Lovelace", "born":1815 }' http://127.0.0.1:5001/alovelace

curl -H "Authorization: Basic user1" -H "Content-type: application/json" -X POST -d '{"first":"Alan","last":"Turing", "born":1912 }' http://127.0.0.1:5001/aturing
```

get keys
```
curl -H "Authorization: Basic user1" http://127.0.0.1:5001/

curl -H "Authorization: Basic user1" "http://127.0.0.1:5001/?prefix=al"
```

get object for key
```
curl -H "Authorization: Basic user1" http://127.0.0.1:5001/aturing
```

delete object for key
```
curl -H "Authorization: Basic user1" -X DELETE http://127.0.0.1:5001/key2
```


set cache max age
```
curl -H "Authorization: Basic user1" -H "Content-type: application/json" -X POST -d '{"k":"v","k2":"v2"}' "http://127.0.0.1:5001/key2?cache_max_age=3600"
```

get only some field
```
curl -H "Authorization: Basic user1" "http://127.0.0.1:5001/aturing?mask=first,last"
```

get keys with values
```
curl -H "Authorization: Basic user1" "http://127.0.0.1:5001/?prefix=a&limit=1&skip=0&reverse=true&values=true&mask=last"
```

