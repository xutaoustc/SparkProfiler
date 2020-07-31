package com.ctyun.sparkprofiler.core

import com.ctyun.sparkprofiler.core.analyzer.AppAnalyzer
import com.ctyun.sparkprofiler.core.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.ctyun.sparkprofiler.core.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.spark.scheduler._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class EventHistoryListener() extends SparkListener{
  private var isStreamingApp     = false
  protected val appInfo          = ApplicationInfo()

  protected val jobMap           = new mutable.HashMap[Long, JobTimeSpan]
  protected val stageIDToJobID   = new mutable.HashMap[Int, Long]
  protected val jobSQLExecID     = new mutable.HashMap[Long, Long]

  protected val stageMap         = new mutable.HashMap[Int, StageTimeSpan]

  protected val hostMap          = new mutable.HashMap[String, HostTimeSpan]()
  protected val executorMap      = new mutable.HashMap[String, ExecutorTimeSpan]()

  protected val appMetrics       = new AggregateMetrics()


  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appInfo.applicationID = applicationStart.appId.getOrElse("NA")
    appInfo.appName = applicationStart.appName
    appInfo.sparkUser = applicationStart.sparkUser
    appInfo.startTime     = applicationStart.time
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    // we now do not want to analyze streaming app now
    if(isStreamingApp)
      return

    appInfo.endTime = applicationEnd.time

    jobMap.foreach{
      case (_, jobTimeSpan) => {
        if (jobTimeSpan.endTime == 0)
          if (!jobTimeSpan.stageMap.isEmpty)
            jobTimeSpan.setEndTime(jobTimeSpan.stageMap.map(y => y._2.endTime).max)
          else
            jobTimeSpan.setEndTime(appInfo.endTime)
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


  override def onJobStart(jobStart: SparkListenerJobStart) {
    if(jobStart.properties.getProperty("callSite.long").contains("StreamingContext")){
      isStreamingApp = true
    }


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


  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    executorMap.getOrElseUpdate(executorAdded.executorId, {
        val timeSpan = new ExecutorTimeSpan(executorAdded.executorId,
                            executorAdded.executorInfo.executorHost,
                            executorAdded.executorInfo.totalCores)
        timeSpan.setStartTime(executorAdded.time)
        timeSpan
    })

    hostMap.getOrElseUpdate(executorAdded.executorInfo.executorHost, {
        val executorHostTimeSpan = new HostTimeSpan(executorAdded.executorInfo.executorHost)
        executorHostTimeSpan.setStartTime(executorAdded.time)
        executorHostTimeSpan
    })
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val executorTimeSpan = executorMap(executorRemoved.executorId)
    executorTimeSpan.setEndTime(executorRemoved.time)
    //We don't get any event for host. Will not try to check when the hosts go out of service
  }


  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskMetrics = taskEnd.taskMetrics
    val taskInfo    = taskEnd.taskInfo

    if (taskMetrics == null) return

    appMetrics.updateAggregateTaskMetrics(taskMetrics, taskInfo)
    hostMap.get(taskInfo.host).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))
    executorMap.get(taskInfo.executorId).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))
    stageIDToJobID.get(taskEnd.stageId).foreach(jobMap.get(_).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo)))
    stageMap.get(taskEnd.stageId).foreach(stageTimeSpan=>{
      stageTimeSpan.updateAggregateTaskMetrics(taskMetrics, taskInfo);
      stageTimeSpan.updateTasks(taskInfo, taskMetrics)}
    )

    if (taskEnd.taskInfo.failed) {
      // TODO
    }
  }


  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageMap.getOrElseUpdate(stageSubmitted.stageInfo.stageId, {
        val stageTimeSpan = new StageTimeSpan(stageSubmitted.stageInfo.stageId,
                                  stageSubmitted.stageInfo.numTasks)
        stageTimeSpan.setParentStageIDs(stageSubmitted.stageInfo.parentIds)
        if (stageSubmitted.stageInfo.submissionTime.isDefined) {
          stageTimeSpan.setStartTime(stageSubmitted.stageInfo.submissionTime.get)
        }

        stageTimeSpan
    })
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

}
