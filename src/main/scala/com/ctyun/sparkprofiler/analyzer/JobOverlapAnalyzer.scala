package com.ctyun.sparkprofiler.analyzer

import com.ctyun.sparkprofiler.common.AppContext
import com.ctyun.sparkprofiler.timespan.JobTimeSpan
import com.ctyun.sparkprofiler.util.JobOverlapHelper

import scala.collection.mutable

class JobOverlapAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val printDetailedReport = true
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()
    out.println ("\nChecking for job overlap...\n")

    var lastEndTime:Long  = 0
    val jobsList = JobOverlapHelper.makeJobLists(ac)
    var count = 1
    val conflictingJobGroups = new mutable.ListBuffer[( Long, Long)]

    jobsList.sortWith( (a, b) => a.map( x => x.startTime).min < b.map( x => x.startTime).min )
      .foreach( x => {
        val minStartTime = x.map( x => x.startTime).min
        val maxEndTime   = x.map( x => x.endTime).max
        val jobIDList = x.map(x => x.jobID)
        val groupID = {
          if (ac.jobSQLExecIdMap.contains(jobIDList.last)) {
            ac.jobSQLExecIdMap(jobIDList.last)
          }else {
            -1
          }
        }
        if (printDetailedReport) {
          out.println(" ")
          out.println(s" JobGroup ${count}  SQLExecID (${groupID})")
          out.println(s" Number of Jobs ${jobIDList.size}  JobIDs(${jobIDList.mkString(",")})")
          out.println(s" Timing [${pt(minStartTime)} - ${pt(maxEndTime)}]")
          out.println(s" Duration  ${pd(maxEndTime - minStartTime)}")
          out.println(" ")

          if (jobIDList.size > 1) {
            printJobGroupTimeLine(out, x)
          }
          x.sortWith((a, b) => a.jobID < b.jobID).foreach(j => {
            out.println(s" JOB ${j.jobID} Start ${pt(j.startTime)}  End ${pt(j.endTime)}")
          })
          out.println(" ")
        }

        if (lastEndTime != 0) {
          if (lastEndTime > minStartTime) {
            conflictingJobGroups += ((count, count-1))
          }
        }
        lastEndTime = maxEndTime
        count += 1
    })

    if (conflictingJobGroups.size > 0) {
      out.println(s"Found ${conflictingJobGroups.size} overlapping JobGroups. Using threadpool for submitting parallel jobs? " +
        s"Some calculations might not be reliable.")
      conflictingJobGroups.foreach( x => {
        out.println(s"Running with overlap:  JobGroupID ${x._1} && JobGroupID ${x._2} ")
      })
    }else {
      out.println ("\nNo overlapping jobgroups found. Good\n")
    }
    out.println("\n")
    out.toString()
  }

  def printJobGroupTimeLine(out: mutable.StringBuilder, listJobTimeSpan: List[JobTimeSpan]): Unit = {

    val startTime = listJobTimeSpan.map( x => x.startTime).min
    val endTime = listJobTimeSpan.map(x => x.endTime).max
    val unit = {
      val x = (endTime-startTime)
      if (x <= 80) {
        1
      }else {
        x/80.toDouble
      }
    }

    listJobTimeSpan.filter(x => x.isFinished())
      .map(x => (x.jobID,
        (x.startTime-startTime)/unit,     //start position
        (x.endTime - startTime)/unit,
        x.stageMap.map(x => x._2.taskExecutionTimes.length).sum,
        x.stageMap.size
      ))    //end position
      .toBuffer.sortWith( (a, b) => a._1 < b._1)
      .foreach( x => {
        val (jobID, start, end, totalTasks, stages) = x
        out.print(f"[JOBID ${jobID}%7s ")
        out.print(" " * start.toInt)
        out.print("|" * (end.toInt - start.toInt))
        if (80 > end) {
          out.print(" " * (80 - end.toInt))
        }
        out.println("]")
      })
  }
}


