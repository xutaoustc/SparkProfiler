package com.ctyun.sparkprofiler.core.timespan

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import org.json4s.DefaultFormats
import scala.collection.mutable

class StageTimeSpan(val stageID: Int, numberOfTasks: Long) extends TimeSpan {
  var parentStageIDs:Seq[Int] = null

  var tempTaskTimes = new mutable.ListBuffer[( Long, Long, Long)]
  var minTaskLaunchTime = Long.MaxValue
  var maxTaskFinishTime = 0L
  var taskExecutionTimes  = Array.emptyIntArray
  var taskPeakMemoryUsage = Array.emptyLongArray


  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats

    Map(
      "stageID" -> stageID,
      "numberOfTasks" -> numberOfTasks,
      "stageMetrics" -> metrics.getMap(),
      "minTaskLaunchTime" -> minTaskLaunchTime,
      "maxTaskFinishTime" -> maxTaskFinishTime,
      "parentStageIDs" -> parentStageIDs.mkString("[", ",", "]"),
      "taskExecutionTimes" -> taskExecutionTimes.mkString("[", ",", "]"),
      "taskPeakMemoryUsage" -> taskPeakMemoryUsage.mkString("[", ",", "]")
    ) ++ super.getStartEndTime()
  }


  def setParentStageIDs(parentIDs: Seq[Int]): Unit = {
    parentStageIDs = parentIDs
  }

  def updateTasks(taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit = {
    if (taskInfo != null && taskMetrics != null) {
      tempTaskTimes += ((taskInfo.taskId, taskMetrics.executorRunTime, taskMetrics.peakExecutionMemory))
      if (taskInfo.launchTime < minTaskLaunchTime) {
        minTaskLaunchTime = taskInfo.launchTime
      }
      if (taskInfo.finishTime > maxTaskFinishTime) {
        maxTaskFinishTime = taskInfo.finishTime
      }
    }
  }

  def finalUpdate(): Unit = {
    //min time for stage is when its tasks started not when it is submitted
    setStartTime(minTaskLaunchTime)
    setEndTime(maxTaskFinishTime)

    taskExecutionTimes = tempTaskTimes.sortWith(( left, right)  => left._1 < right._1).map(_._2.toInt).toArray

    val countPeakMemoryUsage = {
      if (tempTaskTimes.size > 64) {
        64
      }else {
        tempTaskTimes.size
      }
    }

    taskPeakMemoryUsage = tempTaskTimes
      .map( x => x._3)
      .sortWith( (a, b) => a > b)
      .take(countPeakMemoryUsage).toArray

    /*
    Clean the tempTaskTimes. We don't want to keep all this objects hanging around for
    long time
     */
    tempTaskTimes.clear()
  }


}

