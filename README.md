# WatDiv Benchmark with 100M triples

## Queries

Located at `gs://scylla-rdf-benchmark/queries` folder. All the request done by curl, an example command:

```
curl -w "@curl-format.txt" -o results/F1-5.txt \
    --header "Content-Type: application/sparql-query" \
    --data 'SELECT ?v0 ?v2 ?v3 ?v4 ?v5 WHERE {?v0 <http://ogp.me/ns#tag> <http://db.uwaterloo.ca/~galuc/wsdbm/Topic88> .?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v2 .?v3 <http://schema.org/trailer> ?v4 .?v3 <http://schema.org/keywords> ?v5 .?v3 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v0 .?v3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2> .}' \
    --request "POST" "http://graph-worker-vis:3001/server/repositories/watdiv"
```

## Results

| Query  | Time (sec) | Average Time (sec) |
|--------|------------|--------------------|
| L1     | 0.372,0.222,0.487,0.677,0.107 | 0.373 |
| S1 | 1.074 | 1.074 |
| F1 | 19.443, 12.124, 10.913, 11.439, 11.019 | 12.9876 |
| C1 | 222.769, 60.993, 58.618, 57.326, 150.406 | 110.0224 |