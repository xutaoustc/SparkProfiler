package com.ctyun.sparkprofiler.core.common

import java.util.Locale

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

import scala.collection.mutable


class AggregateMetrics() {
  var count = 0L
  val map = new mutable.LinkedHashMap[AggregateMetrics.Metric, AggregateValue]()

  @transient val formatterMap = new mutable.HashMap[AggregateMetrics.Metric,
                                                    ((AggregateMetrics.Metric, AggregateValue), mutable.StringBuilder) => Unit
                                                   ]()

  formatterMap(AggregateMetrics.taskDuration)= formatMillisTime
  formatterMap(AggregateMetrics.executorRuntime) = formatMillisTime
  formatterMap(AggregateMetrics.executorCpuTime)= formatNanoTime
  formatterMap(AggregateMetrics.jvmGCTime) = formatMillisTime

  formatterMap(AggregateMetrics.resultSize)= formatBytes
  formatterMap(AggregateMetrics.inputBytesRead)= formatBytes
  formatterMap(AggregateMetrics.outputBytesWritten)= formatBytes
  formatterMap(AggregateMetrics.memoryBytesSpilled)= formatBytes
  formatterMap(AggregateMetrics.diskBytesSpilled)= formatBytes
  formatterMap(AggregateMetrics.peakExecutionMemory)= formatBytes

  formatterMap(AggregateMetrics.shuffleWriteTime) = formatNanoTime
  formatterMap(AggregateMetrics.shuffleWriteBytesWritten) = formatBytes
  formatterMap(AggregateMetrics.shuffleWriteRecordsWritten) = formatRecords
  formatterMap(AggregateMetrics.shuffleReadFetchWaitTime) = formatNanoTime
  formatterMap(AggregateMetrics.shuffleReadBytesRead) = formatBytes
  formatterMap(AggregateMetrics.shuffleReadRecordsRead) = formatRecords
  formatterMap(AggregateMetrics.shuffleReadLocalBlocks)= formatRecords
  formatterMap(AggregateMetrics.shuffleReadRemoteBlocks) = formatRecords





  @transient val numberFormatter = java.text.NumberFormat.getIntegerInstance

  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (Math.abs(size) >= 1*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (Math.abs(size) >= 1*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (Math.abs(size) >= 1*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else {
        (size.asInstanceOf[Double] / KB, "KB")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  def toMillis(size:Long): String = {
    val MS  = 1000000L
    val SEC = 1000 * MS
    val MT  = 60 * SEC
    val HR  = 60 * MT

    val (value, unit) = {
      if (size >= 1*HR) {
        (size.asInstanceOf[Double] / HR, "hh")
      } else if (size >= 1*MT) {
        (size.asInstanceOf[Double] / MT, "mm")
      } else if (size >= 1*SEC) {
        (size.asInstanceOf[Double] / SEC, "ss")
      } else {
        (size.asInstanceOf[Double] / MS, "ms")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  def formatNanoTime(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${toMillis(x._2.value)}%20s${toMillis(x._2.min)}%15s${toMillis(x._2.max)}%15s${toMillis(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def formatMillisTime(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    def addUnits(x: Long): String = {
      toMillis(x * 1000000)
    }
    sb.append(f" ${x._1}%-30s${addUnits(x._2.value)}%20s${addUnits(x._2.min)}%15s${addUnits(x._2.max)}%15s${addUnits(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def formatBytes(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${bytesToString(x._2.value)}%20s${bytesToString(x._2.min)}%15s${bytesToString(x._2.max)}%15s${bytesToString(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def formatRecords(x: (AggregateMetrics.Metric, AggregateValue), sb: mutable.StringBuilder): Unit = {
    sb.append(f" ${x._1}%-30s${numberFormatter.format(x._2.value)}%20s${numberFormatter.format(x._2.min)}%15s${numberFormatter.format(x._2.max)}%15s${numberFormatter.format(x._2.mean.toLong)}%20s")
      .append("\n")
  }

  def updateMetric(metric: AggregateMetrics.Metric, newValue: Long) : Unit = {
    val aggregateValue = map.getOrElse(metric, new AggregateValue)
    if (count == 0) {
      map(metric) = aggregateValue
    }
    aggregateValue.value +=  newValue
    aggregateValue.max    = math.max(aggregateValue.max, newValue)
    aggregateValue.min    = math.min(aggregateValue.min, newValue)
    val delta: Double     = newValue - aggregateValue.mean
    aggregateValue.mean  += delta/(count+1)
    aggregateValue.m2 += delta * (newValue - aggregateValue.mean)
    aggregateValue.variance = aggregateValue.m2 / (count+1)
  }

  def updateAggregateTaskMetrics(tm: TaskMetrics, ti: TaskInfo): Unit = {
    updateMetric(AggregateMetrics.taskDuration,             ti.duration)
    updateMetric(AggregateMetrics.executorRuntime,          tm.executorRunTime)
    //updateMetric(AggregateMetrics.executorCpuTime,          tm.executorCpuTime) //Nano to Millis
    updateMetric(AggregateMetrics.jvmGCTime,                tm.jvmGCTime)

    updateMetric(AggregateMetrics.resultSize,               tm.resultSize)
    updateMetric(AggregateMetrics.inputBytesRead,           tm.inputMetrics.bytesRead)
    updateMetric(AggregateMetrics.outputBytesWritten,       tm.outputMetrics.bytesWritten)
    updateMetric(AggregateMetrics.memoryBytesSpilled,       tm.memoryBytesSpilled)
    updateMetric(AggregateMetrics.diskBytesSpilled,         tm.diskBytesSpilled)
    updateMetric(AggregateMetrics.peakExecutionMemory,      tm.peakExecutionMemory)

    updateMetric(AggregateMetrics.shuffleWriteTime,         tm.shuffleWriteMetrics.writeTime)    //Nano to Millis
    updateMetric(AggregateMetrics.shuffleWriteBytesWritten, tm.shuffleWriteMetrics.bytesWritten)
    updateMetric(AggregateMetrics.shuffleWriteRecordsWritten, tm.shuffleWriteMetrics.recordsWritten)
    updateMetric(AggregateMetrics.shuffleReadFetchWaitTime, tm.shuffleReadMetrics.fetchWaitTime)    //Nano to Millis
    updateMetric(AggregateMetrics.shuffleReadBytesRead,     tm.shuffleReadMetrics.totalBytesRead)
    updateMetric(AggregateMetrics.shuffleReadRecordsRead,   tm.shuffleReadMetrics.recordsRead)
    updateMetric(AggregateMetrics.shuffleReadLocalBlocks,   tm.shuffleReadMetrics.localBlocksFetched)
    updateMetric(AggregateMetrics.shuffleReadRemoteBlocks,  tm.shuffleReadMetrics.remoteBlocksFetched)
    count += 1
  }

  def print(caption: String, sb: mutable.StringBuilder):Unit = {
    sb.append(s" AggregateMetrics (${caption}) total measurements ${count} ")
      .append("\n")
    sb.append(f"                NAME                        SUM                MIN           MAX                MEAN         ")
      .append("\n")
    map.toBuffer.foreach[Unit](x => {
      formatterMap(x._1)(x, sb)
    })
  }

  def getMap(): Map[String, Any] = {
    Map("count" -> count, "map" -> map.keys.map(key => (key.toString, map.get(key).get.getMap())).toMap)
  }
}

object AggregateMetrics extends Enumeration {

  type Metric = Value
  val shuffleWriteTime,
  shuffleWriteBytesWritten,
  shuffleWriteRecordsWritten,
  shuffleReadFetchWaitTime,
  shuffleReadBytesRead,
  shuffleReadRecordsRead,
  shuffleReadLocalBlocks,
  shuffleReadRemoteBlocks,
  executorRuntime,
  jvmGCTime,
  executorCpuTime,
  resultSize,
  inputBytesRead,
  outputBytesWritten,
  memoryBytesSpilled,
  diskBytesSpilled,
  peakExecutionMemory,
  taskDuration
  = Value

}
