package cc.datafabric.scyllardf.benchmark

import java.io.File
import java.io.FileReader
import java.io.FileWriter
import kotlin.streams.toList

object WatDivResultComparator {

    @JvmStatic
    public fun main(args: Array<String>) {
        if (args.size < 3) {
            throw IllegalArgumentException()
        }

        val oldSummary = File(args[0])
        val newSummary = File(args[1])
        val diffSummary = File(args[2])

        val oldResults = readWholeSummary(oldSummary)
        val newResults = readWholeSummary(newSummary)

        FileWriter(diffSummary).use { fw ->
            fw.write("Query template,Time type, Stat type, Difference (abs.), Difference (%)\n")

            for (groupId in oldResults.keys) {
                val oldResult = oldResults.getValue(groupId)
                val newResult = newResults.getValue(groupId)

                fw.write("$groupId,ttfb,min,")
                fw.write("${computeAbsDiff(oldResult.minTTFB(), newResult.minTTFB())},")
                fw.write("${computeRelDiff(oldResult.minTTFB(), newResult.minTTFB())}\n")
                fw.write("$groupId,ttfb,mean,")
                fw.write("${computeAbsDiff(oldResult.meanTTFB(), newResult.meanTTFB())},")
                fw.write("${computeRelDiff(oldResult.meanTTFB(), newResult.meanTTFB())}\n")
                fw.write("$groupId,ttfb,std,")
                fw.write("${computeAbsDiff(oldResult.stdTTFB(), newResult.stdTTFB())},")
                fw.write("${computeRelDiff(oldResult.stdTTFB(), newResult.stdTTFB())}\n")
                fw.write("$groupId,ttfb,max,")
                fw.write("${computeAbsDiff(oldResult.maxTTFB(), newResult.maxTTFB())},")
                fw.write("${computeRelDiff(oldResult.maxTTFB(), newResult.maxTTFB())}\n")

                fw.write("$groupId,ttlb,min,")
                fw.write("${computeAbsDiff(oldResult.minTTLB(), newResult.minTTLB())},")
                fw.write("${computeRelDiff(oldResult.minTTLB(), newResult.minTTLB())}\n")
                fw.write("$groupId,ttlb,mean,")
                fw.write("${computeAbsDiff(oldResult.meanTTLB(), newResult.meanTTLB())},")
                fw.write("${computeRelDiff(oldResult.meanTTLB(), newResult.meanTTLB())}\n")
                fw.write("$groupId,ttlb,std,")
                fw.write("${computeAbsDiff(oldResult.stdTTLB(), newResult.stdTTLB())},")
                fw.write("${computeRelDiff(oldResult.stdTTLB(), newResult.stdTTLB())}\n")
                fw.write("$groupId,ttlb,max,")
                fw.write("${computeAbsDiff(oldResult.maxTTLB(), newResult.maxTTLB())},")
                fw.write("${computeRelDiff(oldResult.maxTTLB(),newResult.maxTTLB())}\n")
            }
        }
    }

    private fun computeRelDiff(old: Double, new: Double): String {
        return format((old / new - 1.0) * 100)
    }

    private fun computeAbsDiff(old: Double, new: Double): String {
        return format(old - new)
    }

    private fun format(d: Double): String {
        return String.format("%.4f", d)
    }

    private fun readWholeSummary(file: File): Map<String, SummaryResult> {
        val rows = FileReader(file).readLines().stream().skip(1).toList()

        val map = mutableMapOf<String, SummaryResult>()
        var groupId: String? = null
        var result: SummaryResult? = null
        for (row in rows) {
            val columns = row.split(",")
            if (groupId != columns[0]) {
                if (result != null) {
                    map[groupId!!] = result
                }

                groupId = columns[0]
                result = SummaryResult(groupId)
            }

            result!!.addValue("${columns[1]}-${columns[2]}", columns[3].toDouble())
        }

        return map
    }

    private data class SummaryResult(val groupId: String) {

        private val values = mutableMapOf<String, Double>()

        fun addValue(key: String, value: Double) {
            values[key] = value
        }

        fun minTTFB(): Double {
            return values["ttfb-min"]!!
        }

        fun meanTTFB(): Double {
            return values["ttfb-mean"]!!
        }

        fun stdTTFB(): Double {
            return values["ttfb-std"]!!
        }

        fun maxTTFB(): Double {
            return values["ttfb-max"]!!
        }

        fun minTTLB(): Double {
            return values["ttlb-min"]!!
        }

        fun meanTTLB(): Double {
            return values["ttlb-mean"]!!
        }

        fun stdTTLB(): Double {
            return values["ttlb-std"]!!
        }

        fun maxTTLB(): Double {
            return values["ttlb-max"]!!
        }

    }

}