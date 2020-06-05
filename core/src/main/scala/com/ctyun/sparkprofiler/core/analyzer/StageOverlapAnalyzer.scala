package com.ctyun.sparkprofiler.core.analyzer

import com.ctyun.sparkprofiler.core.common.AppContext

import scala.collection.mutable

class StageOverlapAnalyzer extends  AppAnalyzer {

  /*
  TODO: Delete this code. We handle parallel stages now
   */
  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println ("\nChecking for stage overlap...\n")
    val conflictingStages = new mutable.ListBuffer[( Long, Int, Int)]
    val jobids = ac.jobMap.keySet.toBuffer.sortWith( _ < _ )
    jobids.foreach( jobID => {
      val jobTimeSpan = ac.jobMap(jobID)
      val stageids = jobTimeSpan.stageMap.keySet.toBuffer.sortWith( _ < _ )

      var lastStageEndTime = 0L
      var lastStageID = 0
      stageids.foreach( stageID => {
        val sts = jobTimeSpan.stageMap(stageID)
        if (sts.endTime > 0 && sts.startTime > 0 ) {
          if (sts.startTime < lastStageEndTime) {
            conflictingStages += ((jobID, stageID, lastStageID))
          }
          lastStageEndTime = sts.endTime
          lastStageID = stageID
        }
      })
    })

    if (conflictingStages.size > 0) {
      out.println(s"Found ${conflictingStages.size} overlapping stages. Some calculations might not be reliable.")
      conflictingStages.foreach( x => {
        out.println(s"In Job ${x._1} stages ${x._2} & ${x._3}")
      })
    }else {
      out.println ("\nNo overlapping stages found. Good\n")
    }
    out.println("\n")
    out.toString()
  }
}
