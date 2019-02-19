package cc.datafabric.scyllardf.benchmark

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.Options
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
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import java.util.LinkedList
import kotlin.system.exitProcess


public object WatDivQueryExecutor {

    private const val OUTPUT_FILE_SUFFIX = ".txt"
    private const val EXECUTION_TIMEOUT = 1000L
    private const val DEFAULT_POOL_SIZE = 1
    private const val DEFAULT_ITERATIONS = 10

    private val log = LoggerFactory.getLogger(WatDivQueryExecutor::class.java)

    private lateinit var client: CloseableHttpClient

    private lateinit var url: URI
    private var poolSize: Int = DEFAULT_POOL_SIZE
    private lateinit var requestTimingHandler: RequestTimingHandler

    private fun executeWarmUp(query: String, outputFile: File) {
        val request = HttpPost(url)
        request.entity = StringEntity(query, ContentType.create("application/sparql-query", Charsets.UTF_8))

        val response = client.execute(request)
        val body = EntityUtils.toString(response.entity, Charsets.UTF_8)
        FileWriter(outputFile).use {
            it.write(body)
        }
        EntityUtils.consume(response.entity)
        response.close()
    }

    object Cli {
        private val options: Options = Options().apply {
            addRequiredOption("u", "url", true, "SPARQL endpoint")
            addRequiredOption("q", "queries", true, "directory where each file represent one query")
            addRequiredOption("o", "output", true, "directory where results should be stored")
            addOption("N", "pool size")
        }

        private val parser = DefaultParser()

        fun parse(args: Array<String>): CommandLine = parser.parse(options, args)

        fun printHelp() = HelpFormatter().printHelp(this.javaClass.simpleName, options)
    }

    // TODO use production-ready module to acquire stats (JMX? kotlin-statistics?)
    private fun execute(queryId: String, query: String) {
        val request = HttpPost(url)
        request.entity = StringEntity(query, ContentType.create("application/sparql-query", Charsets.UTF_8))
        val stats = mutableListOf<Pair<Long, Long>>()
        runBlocking {
            repeat(poolSize) {
                GlobalScope.launch {
                    // Time to first byte
                    val ttfbs = mutableListOf<Long>()

                    // Time to last byte
                    val ttlbs = mutableListOf<Long>()
                    repeat(DEFAULT_ITERATIONS) {
                        val start = System.nanoTime()
                        client.execute(request).also {
                            ttfbs.add(System.nanoTime() - start)

                            // wait until all bytes are received
                            it.entity.content.readBytes()

                            ttlbs.add(System.nanoTime() - start)
                        }
                    }
                    stats[it] = Pair(ttfbs.sum() / ttfbs.size, ttlbs.sum() / ttlbs.size)
                }
            }
        }
        requestTimingHandler.handle(
            queryId,
            stats.map { it.first }.sum() / poolSize,
            stats.map { it.second }.sum() / poolSize
        )
    }

    @JvmStatic
    public fun main(args: Array<String>) = runBlocking {
        val cmd: CommandLine
        try {
            cmd = Cli.parse(args)
        } catch (ex: MissingOptionException) {
            Cli.printHelp()
            exitProcess(1)
        }
        url = URI.create(cmd.getOptionValue("u"))
        val queriesDir = File(cmd.getOptionValue("q"))
        val outputDir = File(cmd.getOptionValue("o"))
        if (cmd.hasOption("N")) {
            poolSize = cmd.getOptionValue("N").toInt()
        }


        if (!queriesDir.isDirectory || !queriesDir.exists()) {
            throw IllegalArgumentException("The queries directory doesn't exists!")
        }

        if (!outputDir.exists()) {
            println("Output directory is missing. Creating ${outputDir.absolutePath} ...")
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
                    val outputFile = Files.createFile(
                        Paths.get(
                            outputDir.absolutePath,
                            "${it.nameWithoutExtension}-$queryId$OUTPUT_FILE_SUFFIX"
                        )
                    ).toFile()
                    executeWarmUp(query, outputFile)
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
                    log.info("Executing {}th query...", queryId)
                    execute(queryId.toString(), query)
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