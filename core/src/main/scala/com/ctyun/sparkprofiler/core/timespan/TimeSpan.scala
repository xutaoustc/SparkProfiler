package com.ctyun.sparkprofiler.core.timespan

import com.ctyun.sparkprofiler.core.common.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue


trait TimeSpan  {
  var startTime: Long = 0
  var endTime: Long = 0

  def setStartTime(time: Long): Unit = {
    startTime = time
  }
  def setEndTime(time: Long): Unit = {
    endTime = time
  }

  def duration(): Option[Long] = {
    if (isFinished()) {
      Some(endTime - startTime)
    } else {
      None
    }
  }
  def isFinished(): Boolean = (endTime != 0 && startTime != 0)


  var metrics = new AggregateMetrics()
  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    metrics.updateAggregateTaskMetrics(taskMetrics, taskInfo)
  }
}
