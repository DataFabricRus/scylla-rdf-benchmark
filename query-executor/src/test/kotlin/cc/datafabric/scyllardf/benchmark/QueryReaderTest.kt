package cc.datafabric.scyllardf.benchmark

import org.junit.jupiter.api.Test
import java.nio.file.Paths
import kotlin.test.assertEquals

class QueryReaderTest {

    @Test
    public fun test() {
        val file = Paths
            .get(QueryReaderTest::class.java.getResource("/cc/datafabric/scyllardf/benchmark/query.sparql").toURI())
            .toFile()

        val queries = WatDivQueryExecutor.QueryReader(file).readAll()

        assertEquals(5, queries.size)
    }

}