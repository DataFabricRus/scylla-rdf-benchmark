package cc.datafabric.scyllardf.benchmark

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.util.LinkedList


public object WatDivQueryExecutor {

    private const val OUTPUT_FILE_SUFFIX = ".txt"
    private const val EXECUTION_TIMEOUT = 1000L

    private val log = LoggerFactory.getLogger(WatDivQueryExecutor::class.java)

    private lateinit var client: CloseableHttpClient

    private lateinit var url: String
    private lateinit var requestTimingHandler: RequestTimingHandler

    private fun executeWarmUp(query: String) {
        val request = HttpPost(url)
        request.entity = StringEntity(query, ContentType.create("application/sparql-query", Charsets.UTF_8))

        val response = client.execute(request)
        EntityUtils.consume(response.entity)
        response.close()
    }

    private fun execute(queryId: String, query: String, outputFile: File) {
        val request = HttpPost(url)
        request.entity = StringEntity(query, ContentType.create("application/sparql-query", Charsets.UTF_8))

        val start = System.nanoTime()
        client.execute(request).use { response ->
            val ttfb = System.nanoTime() - start

            val body = EntityUtils.toString(response.entity, Charsets.UTF_8)
            val ttlb = System.nanoTime() - start

            FileWriter(outputFile).use {
                it.write(body)
            }

            requestTimingHandler.handle(queryId, ttfb, ttlb)
        }
    }

    @JvmStatic
    public fun main(args: Array<String>) {
        if (args.size < 3) {
            throw IllegalArgumentException("Requires at least 3 arguments: `url`, `queries` and `output` locations!")
        }

        url = args[0]

        val queriesDir = File(args[1])
        val outputDir = File(args[2])

        if (!queriesDir.isDirectory || !queriesDir.exists()) {
            throw IllegalArgumentException("The queries directory doesn't exists!")
        }

        if (!outputDir.exists()) {
            outputDir.mkdir()
        }

        val requestConfig = RequestConfig.custom()
            .setConnectTimeout(0)
            .setConnectionRequestTimeout(0)
            .setSocketTimeout(0)
            .build()
        client = HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .build()

        requestTimingHandler = RequestTimingHandler(
            Files.createFile(Paths.get(outputDir.absolutePath, "time.csv")).toFile(),
            Files.createFile(Paths.get(outputDir.absolutePath, "summary.csv")).toFile()
        )

        log.info("Running warm up queries...")

        queriesDir.listFiles().forEach {
            QueryReader(it).readAll().take(1).forEachIndexed { queryId, query ->
                if (queryId == 0) {
                    log.info("Executing a warm up query from ${it.nameWithoutExtension}...")
                    executeWarmUp(query)
                }

                Thread.sleep(EXECUTION_TIMEOUT)
            }
        }

        log.info("Running benchmark queries...")

        queriesDir.listFiles().forEach {
            requestTimingHandler.startGroup(it.nameWithoutExtension)
            log.info("Started query template {}", it.nameWithoutExtension)

            QueryReader(it).readAll().forEachIndexed { queryId, query ->
                if (queryId != 0) {
                    val outputFile = Files.createFile(Paths.get(outputDir.absolutePath,
                        "${it.nameWithoutExtension}-$queryId$OUTPUT_FILE_SUFFIX")).toFile()

                    log.info("Executing {}th query...", queryId)
                    execute(queryId.toString(), query, outputFile)
                }

                Thread.sleep(EXECUTION_TIMEOUT)
            }

            requestTimingHandler.finishGroup()
        }

        log.info("Finish the benchmark!")
    }

    public class QueryReader(private val file: File) {

        companion object {
            private val PATTERN_EMPTY_LINE = Regex("^\\s*\\n", setOf(RegexOption.MULTILINE, RegexOption.UNIX_LINES))
        }

        fun readAll(): List<String> {
            return file.readText(Charsets.UTF_8).split(PATTERN_EMPTY_LINE).filter { !it.isBlank() }
        }

    }

    class RequestTimingHandler(private val outputTimings: File, private val outputSummary: File) {

        companion object {
            private const val NANO_SECONDS = 1000000000L
        }

        private var ttfbStats = DescriptiveStatistics()
        private var ttlbStats = DescriptiveStatistics()
        private var timings = LinkedList<String>()

        private lateinit var groupId: String

        fun startGroup(id: String) {
            groupId = id
            timings.clear()
            ttfbStats.clear()
            ttlbStats.clear()
        }

        fun handle(queryId: String, timeToFirstByte: Long, timeToLoadBody: Long) {
            val ttfb = timeToFirstByte.toDouble() / NANO_SECONDS
            val ttlb = timeToLoadBody.toDouble() / NANO_SECONDS

            ttfbStats.addValue(ttfb)
            ttlbStats.addValue(ttlb)

            timings.add("$groupId,$queryId,${String.format("%.4f", ttfb)},${String.format("%.4f", ttlb)}")
        }

        fun finishGroup() {
            FileWriter(outputTimings, true).use { fw ->
                timings.forEach {
                    fw.write(it)
                    fw.write("\n")
                }
            }
            FileWriter(outputSummary, true).use {
                it.write("$groupId,ttfb,min,${String.format("%.4f", ttfbStats.min)}\n")
                it.write("$groupId,ttfb,mean,${String.format("%.4f", ttfbStats.mean)}\n")
                it.write("$groupId,ttfb,std,${String.format("%.4f", ttfbStats.standardDeviation)}\n")
                it.write("$groupId,ttfb,max,${String.format("%.4f", ttfbStats.max)}\n")

                it.write("$groupId,ttlb,min,${String.format("%.4f", ttlbStats.min)}\n")
                it.write("$groupId,ttlb,mean,${String.format("%.4f", ttlbStats.mean)}\n")
                it.write("$groupId,ttlb,std,${String.format("%.4f", ttlbStats.standardDeviation)}\n")
                it.write("$groupId,ttlb,max,${String.format("%.4f", ttlbStats.max)}\n")
            }
        }

    }

}