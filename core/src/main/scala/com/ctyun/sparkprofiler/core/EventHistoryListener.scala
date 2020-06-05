package com.ctyun.sparkprofiler.core

import com.ctyun.sparkprofiler.core.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.ctyun.sparkprofiler.core.analyzer.AppAnalyzer
import com.ctyun.sparkprofiler.core.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.ctyun.sparkprofiler.core.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.ctyun.sparkprofiler.core.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.spark.scheduler._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


// 监听以下事件
//   onApplicationStart, onApplicationEnd         ->  ApplicationInfo
//   onExecutorAdded, onExecutorRemoved           ->  ExecutorTimeSpan,HostTimeSpan
//   onJobStart, onJobEnd                         ->  JobTimeSpan，stageIDToJobID，jobSQLExecID
//      onStageSubmitted     ->  StageTimeSpan增
//      onStageCompleted     ->  加StageTimeSpan到JobTimeSpan中，StageTimeSpan最后确定
//   onTaskEnd               ->  更新Host, Executor, Job, Stage指标

class EventHistoryListener() extends SparkListener{
  protected val appInfo          = new ApplicationInfo()

  protected val hostMap          = new mutable.HashMap[String, HostTimeSpan]()
  protected val executorMap      = new mutable.HashMap[String, ExecutorTimeSpan]()

  protected val jobMap           = new mutable.HashMap[Long, JobTimeSpan]
  protected val stageIDToJobID   = new mutable.HashMap[Int, Long]
  protected val jobSQLExecID  = new mutable.HashMap[Long, Long]

  protected val stageMap         = new mutable.HashMap[Int, StageTimeSpan]
  protected val failedStages     = new ListBuffer[String]

  protected val appMetrics       = new AggregateMetrics()




  // 在SparkContext初始化结束之后
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appInfo.applicationID = applicationStart.appId.getOrElse("NA")
    appInfo.startTime     = applicationStart.time
  }

  // 在整个Spark应用结束之时
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    appInfo.endTime = applicationEnd.time

    //如果可能，补上JobTimeSpan的endTime
    jobMap.foreach{
      case (_, jobTimeSpan) => {
        if (jobTimeSpan.endTime == 0) {
          if (!jobTimeSpan.stageMap.isEmpty) {
            jobTimeSpan.setEndTime(jobTimeSpan.stageMap.map(y => y._2.endTime).max)
          }else {
            jobTimeSpan.setEndTime(appInfo.endTime)
          }
        }
    }}

    val appContext = new AppContext(appInfo,
      appMetrics,
      hostMap,
      executorMap,
      jobMap,
      jobSQLExecID,
      stageMap,
      stageIDToJobID)
    AppAnalyzer.startAnalyzers(appContext)
  }



  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    executorMap.getOrElseUpdate(
        executorAdded.executorId,
        {
            val timeSpan = new ExecutorTimeSpan(executorAdded.executorId,
                                                executorAdded.executorInfo.executorHost,
                                                executorAdded.executorInfo.totalCores)
            timeSpan.setStartTime(executorAdded.time)

            timeSpan
        }
    )

    hostMap.getOrElseUpdate(
        executorAdded.executorInfo.executorHost,
        {
            val executorHostTimeSpan = new HostTimeSpan(executorAdded.executorInfo.executorHost)
            executorHostTimeSpan.setStartTime(executorAdded.time)

            executorHostTimeSpan
        }
    )
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val executorTimeSpan = executorMap(executorRemoved.executorId)
    executorTimeSpan.setEndTime(executorRemoved.time)
    //We don't get any event for host. Will not try to check when the hosts go out of service
  }



  // 生成Stage和Job之后，提交Stage之前
  override def onJobStart(jobStart: SparkListenerJobStart) {
    val jobTimeSpan = new JobTimeSpan(jobStart.jobId)
    jobTimeSpan.setStartTime(jobStart.time)
    jobMap(jobStart.jobId) = jobTimeSpan

    jobStart.stageIds.foreach( stageID => {
      stageIDToJobID(stageID) = jobStart.jobId
    })

    val sqlExecutionID = jobStart.properties.getProperty("spark.sql.execution.id")
    if (sqlExecutionID != null && !sqlExecutionID.isEmpty) {
      jobSQLExecID(jobStart.jobId) = sqlExecutionID.toLong
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobTimeSpan = jobMap(jobEnd.jobId)
    jobTimeSpan.setEndTime(jobEnd.time)
  }



  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageMap.getOrElseUpdate(
        stageSubmitted.stageInfo.stageId,
        {
            val stageTimeSpan = new StageTimeSpan(
                                      stageSubmitted.stageInfo.stageId,
                                      stageSubmitted.stageInfo.numTasks)
            stageTimeSpan.setParentStageIDs(stageSubmitted.stageInfo.parentIds)
            if (stageSubmitted.stageInfo.submissionTime.isDefined) {
              stageTimeSpan.setStartTime(stageSubmitted.stageInfo.submissionTime.get)
            }

            stageTimeSpan
        }
    )
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageTimeSpan = stageMap(stageCompleted.stageInfo.stageId)
    if (stageCompleted.stageInfo.completionTime.isDefined) {
      stageTimeSpan.setEndTime(stageCompleted.stageInfo.completionTime.get)
    }
    if (stageCompleted.stageInfo.submissionTime.isDefined) {
      stageTimeSpan.setStartTime(stageCompleted.stageInfo.submissionTime.get)
    }
    if (stageCompleted.stageInfo.failureReason.isDefined) {
      stageTimeSpan.finalUpdate()
    }else {
      val jobID = stageIDToJobID(stageCompleted.stageInfo.stageId)
      val jobTimeSpan = jobMap(jobID)
      jobTimeSpan.addStage(stageTimeSpan)

      stageTimeSpan.finalUpdate()
    }
  }



  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskMetrics = taskEnd.taskMetrics
    val taskInfo    = taskEnd.taskInfo

    if (taskMetrics == null) return

    //update app metrics
    appMetrics.updateAggregateTaskMetrics(taskMetrics, taskInfo)

    // Host -> Executor -> Job -> Stage
    hostMap.get(taskInfo.host).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))
    executorMap.get(taskInfo.executorId).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))
    stageIDToJobID.get(taskEnd.stageId).foreach(jobID=>{jobMap.get(jobID).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))})
    stageMap.get(taskEnd.stageId).foreach(stageTimeSpan=>{
      stageTimeSpan.updateAggregateTaskMetrics(taskMetrics, taskInfo);
      stageTimeSpan.updateTasks(taskInfo, taskMetrics)}
    )

    if (taskEnd.taskInfo.failed) {
      // TODO
    }
  }

}
