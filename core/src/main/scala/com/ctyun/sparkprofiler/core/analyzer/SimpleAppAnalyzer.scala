package com.ctyun.sparkprofiler.core.analyzer

import com.ctyun.sparkprofiler.core.common.{AggregateMetrics, AppContext}
import com.ctyun.sparkprofiler.sink.Sink
import com.ctyun.sparkprofiler.sink.domain.{SimpleAppSinkInfo, SimpleJobSinkInfo, SimpleStageSinkInfo}


class SimpleAppAnalyzer extends AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): Unit = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)

    Sink.getSink().sinkSimpleApp(
      SimpleAppSinkInfo(
        ac.appInfo.applicationID,
        ac.appInfo.appName,
        ac.appInfo.sparkUser,
        ac.appMetrics.count,
        ac.appMetrics.map(AggregateMetrics.taskDuration).value,
        ac.appMetrics.map(AggregateMetrics.executorRuntime).value,
        ac.appMetrics.map(AggregateMetrics.jvmGCTime).value,
        ac.appMetrics.map(AggregateMetrics.memoryBytesSpilled).value,
        ac.appMetrics.map(AggregateMetrics.diskBytesSpilled).value,
        ac.appMetrics.map(AggregateMetrics.peakExecutionMemory).value,
        ac.appMetrics.map(AggregateMetrics.inputBytesRead).value,
        ac.appMetrics.map(AggregateMetrics.outputBytesWritten).value,
        ac.appMetrics.map(AggregateMetrics.resultSize).value,
        ac.appMetrics.map(AggregateMetrics.shuffleWriteBytesWritten).value,
        ac.appMetrics.map(AggregateMetrics.shuffleWriteRecordsWritten).value,
        ac.appMetrics.map(AggregateMetrics.shuffleWriteTime).value,
        ac.appMetrics.map(AggregateMetrics.shuffleReadFetchWaitTime).value,
        ac.appMetrics.map(AggregateMetrics.shuffleReadBytesRead).value,
        ac.appMetrics.map(AggregateMetrics.shuffleReadRecordsRead).value,
        ac.appMetrics.map(AggregateMetrics.shuffleReadLocalBlocks).value,
        ac.appMetrics.map(AggregateMetrics.shuffleReadRemoteBlocks).value,
        ac.appInfo.startTime,
        ac.appInfo.endTime
      ),
      ac.jobMap.map{
        case (_, jobTimeSpan)=>{
          SimpleJobSinkInfo(
            ac.appInfo.applicationID,
            jobTimeSpan.jobID,
            jobTimeSpan.metrics.count,
            jobTimeSpan.metrics.map(AggregateMetrics.taskDuration).value,
            jobTimeSpan.metrics.map(AggregateMetrics.executorRuntime).value,
            jobTimeSpan.metrics.map(AggregateMetrics.jvmGCTime).value,
            jobTimeSpan.metrics.map(AggregateMetrics.memoryBytesSpilled).value,
            jobTimeSpan.metrics.map(AggregateMetrics.diskBytesSpilled).value,
            jobTimeSpan.metrics.map(AggregateMetrics.peakExecutionMemory).value,
            jobTimeSpan.metrics.map(AggregateMetrics.inputBytesRead).value,
            jobTimeSpan.metrics.map(AggregateMetrics.outputBytesWritten).value,
            jobTimeSpan.metrics.map(AggregateMetrics.resultSize).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleWriteBytesWritten).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleWriteRecordsWritten).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleWriteTime).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleReadFetchWaitTime).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleReadBytesRead).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleReadRecordsRead).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleReadLocalBlocks).value,
            jobTimeSpan.metrics.map(AggregateMetrics.shuffleReadRemoteBlocks).value,
            jobTimeSpan.startTime,
            jobTimeSpan.endTime
          )
        }
      },
      ac.stageMap.map{
        case (_, stageTimeSpan)=>{
          SimpleStageSinkInfo(
            ac.appInfo.applicationID,
            ac.jobMap( ac.stageIDToJobID(stageTimeSpan.stageID)).jobID,
            stageTimeSpan.stageID,
            stageTimeSpan.metrics.count,
            stageTimeSpan.metrics.map(AggregateMetrics.taskDuration).value,
            stageTimeSpan.metrics.map(AggregateMetrics.executorRuntime).value,
            stageTimeSpan.metrics.map(AggregateMetrics.jvmGCTime).value,
            stageTimeSpan.metrics.map(AggregateMetrics.memoryBytesSpilled).value,
            stageTimeSpan.metrics.map(AggregateMetrics.diskBytesSpilled).value,
            stageTimeSpan.metrics.map(AggregateMetrics.peakExecutionMemory).value,
            stageTimeSpan.metrics.map(AggregateMetrics.inputBytesRead).value,
            stageTimeSpan.metrics.map(AggregateMetrics.outputBytesWritten).value,
            stageTimeSpan.metrics.map(AggregateMetrics.resultSize).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleWriteBytesWritten).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleWriteRecordsWritten).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleWriteTime).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleReadFetchWaitTime).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleReadBytesRead).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleReadRecordsRead).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleReadLocalBlocks).value,
            stageTimeSpan.metrics.map(AggregateMetrics.shuffleReadRemoteBlocks).value,
            stageTimeSpan.startTime,
            stageTimeSpan.endTime
          )
        }
      }
    )
  }
}
