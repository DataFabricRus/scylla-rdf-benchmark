# Benchmark for [Scylla-RDF](http://github.com/DataFabricRus/scylla-rdf)

## [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 100M

The queries are executed by the [query-executor](https://github.com/DataFabricRus/scylla-rdf-benchmark/tree/master/query-executor).
Example command:

```
java -jar query-executor-1.0-SNAPSHOT-jar-with-dependencies.jar http://graph-worker-vis:3001/server/repositories/watdiv ./queries ./results
```

## Dataset & Queries

  * dataset (~109M triples) at `gs://scylla-rdf-benchmark/dataset.nt`,
  * queries at `gs://scylla-rdf-benchmark/queries/`.
  
## Metrics

  * `ttfb` - time to first byte
  * `ttlb` - time to load bytes

## Results

  * [v1.0-2018-02-12](https://github.com/DataFabricRus/scylla-rdf-benchmark/tree/master/results/v1.0-2018-02-12)
  * [v1.0-2018-02-11](https://github.com/DataFabricRus/scylla-rdf-benchmark/tree/master/results/v1.0-2018-02-11)