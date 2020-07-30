package com.ctyun.sparkprofiler.core.analyzer

import com.ctyun.sparkprofiler.core.common.AppContext
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
        ac.appInfo.startTime,
        ac.appInfo.endTime
      ),
      ac.jobMap.map{
        case (_, jobTimeSpan)=>{
          SimpleJobSinkInfo(
            ac.appInfo.applicationID,
            jobTimeSpan.jobID,
            jobTimeSpan.metrics.count,
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
            stageTimeSpan.startTime,
            stageTimeSpan.endTime
          )
        }
      }
    )
  }
}
