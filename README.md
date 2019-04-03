# Benchmarks for [Scylla-RDF](http://github.com/DataFabricRus/scylla-rdf)

## Ingestion: Google Dataflow 109M triples

The ingestion pipelines from [scylla-beam-pipelines](https://github.com/DataFabricRus/scylla-beam-pipelines) were used. 
The ingestion included the bulk loading RDF into all indexes in ScyllaDB, the full-text index wasn't considered here.

ScyllaDB had 2 nodes with the following characteristics:

  * n1-standard-16,
  * 2 * Local SSD (375 Gb each)
  
The pipelines were run on Google Dataflow:

  * 20 machines (n1-standard-1),
  * 1 * connection to ScyllaDB per machine,
  * 8192 * parallel requests per a connection.

The ingestion pipelines run ~16 min and loaded 109,836,664 RDF triples which gives as ~114k triples/sec. 

## Queries: [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 109M triples

The queries are executed by the [query-executor](https://github.com/DataFabricRus/scylla-rdf-benchmark/tree/master/query-executor).
Example command:

```
java -jar query-executor-1.0-SNAPSHOT-jar-with-dependencies.jar http://graph-worker-vis:3001/server/repositories/watdiv ./queries ./results
```

### Dataset & Queries

  * dataset (~109M triples) at `gs://scylla-rdf-benchmark/dataset.nt`,
  * queries at `gs://scylla-rdf-benchmark/queries/`.
  
### Metrics

  * `ttfb` - time to first byte
  * `ttlb` - time to load bytes

### Results

  * [v0.0.1-2019-02-12](https://github.com/DataFabricRus/scylla-rdf-benchmark/tree/master/results/v0.0.1-2019-02-12)
  * [v0.0.1-2019-02-11](https://github.com/DataFabricRus/scylla-rdf-benchmark/tree/master/results/v0.0.1-2019-02-11)
