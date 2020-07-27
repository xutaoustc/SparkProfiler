package com.ctyun.sparkprofiler.core.timespan

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo
import scala.collection.mutable

class StageTimeSpan(val stageID: Int, numberOfTasks: Long) extends TimeSpan {
  var parentStageIDs:Seq[Int] = null
  def setParentStageIDs(parentIDs: Seq[Int]): Unit = {
    parentStageIDs = parentIDs
  }


  var tempTaskTimes = new mutable.ListBuffer[( Long, Long, Long)]
  var minTaskLaunchTime = Long.MaxValue
  var maxTaskFinishTime = 0L

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


  var taskExecutionTimes  = Array.emptyIntArray
  var taskPeakMemoryUsage = Array.emptyLongArray

  def finalUpdate(): Unit = {
    setStartTime(minTaskLaunchTime)
    setEndTime(maxTaskFinishTime)

    taskExecutionTimes = tempTaskTimes.sortWith(( left, right)  => left._1 < right._1).map(_._2.toInt).toArray
    val countPeakMemoryUsage = if (tempTaskTimes.size > 64) 64 else tempTaskTimes.size
    taskPeakMemoryUsage = tempTaskTimes.map( x => x._3).sortWith( (a, b) => a > b).take(countPeakMemoryUsage).toArray

    tempTaskTimes.clear()
  }
}

