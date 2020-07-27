package com.ctyun.sparkprofiler.core.common

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import scala.collection.mutable

class AggregateMetrics() {
  var count = 0L
  val map = new mutable.LinkedHashMap[AggregateMetrics.Metric, AggregateValue]()


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

  private def updateMetric(metric: AggregateMetrics.Metric, newValue: Long) : Unit = {
    val aggregateValue = map.getOrElseUpdate(metric, new AggregateValue)
    aggregateValue.value +=  newValue
    aggregateValue.max    = math.max(aggregateValue.max, newValue)
    aggregateValue.min    = math.min(aggregateValue.min, newValue)
    val delta: Double     = newValue - aggregateValue.mean
    aggregateValue.mean  += delta/(count+1)
    aggregateValue.m2    += delta * (newValue - aggregateValue.mean)
    aggregateValue.variance = aggregateValue.m2 / (count+1)
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
