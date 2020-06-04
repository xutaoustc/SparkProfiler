package com.ctyun.sparkprofiler.timespan

import com.ctyun.sparkprofiler.common.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue


trait TimeSpan  {
  var startTime: Long = 0
  var endTime: Long = 0

  var metrics = new AggregateMetrics()

  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    metrics.update(taskMetrics, taskInfo)
  }

  def setStartTime(time: Long): Unit = {
    startTime = time
  }
  def setEndTime(time: Long): Unit = {
    endTime = time
  }
  def addStartEnd(json: JValue): Unit = {
    implicit val formats = DefaultFormats
    this.startTime = (json \ "startTime").extract[Long]
    this.endTime = (json \ "endTime").extract[Long]
  }


  def duration(): Option[Long] = {
    if (isFinished()) {
      Some(endTime - startTime)
    } else {
      None
    }
  }
  def isFinished(): Boolean = (endTime != 0 && startTime != 0)



  def getMap(): Map[String, _ <: Any]

  def getStartEndTime(): Map[String, Long] = {
    Map("startTime" -> startTime, "endTime" -> endTime)
  }
}
