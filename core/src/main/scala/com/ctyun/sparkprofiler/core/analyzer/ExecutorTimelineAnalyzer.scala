package com.ctyun.sparkprofiler.core.analyzer

import com.ctyun.sparkprofiler.core.common.AppContext
import com.ctyun.sparkprofiler.core.common.AppContext
import com.ctyun.sparkprofiler.core.timespan.ExecutorTimeSpan

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ExecutorTimelineAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    out.println("\nPrinting executors timeline....\n")
    out.println(s"Total Executors ${ac.executorMap.size}, " +
      s"and maximum concurrent executors = ${AppContext.getMaxConcurrent(ac.executorMap, ac)}")

    val minuteExecutorMap = new mutable.HashMap[String, (ListBuffer[ExecutorTimeSpan], ListBuffer[ExecutorTimeSpan])]()

    ac.executorMap.values
      .foreach( x => {
        val startMinute = MINUTES_DF.format(x.startTime)
        val minuteLists = minuteExecutorMap.getOrElseUpdate(startMinute, (new mutable.ListBuffer[ExecutorTimeSpan](), new mutable.ListBuffer[ExecutorTimeSpan]()))
        minuteLists._1 += x
        if (x.endTime != 0) {
          val endMinute = MINUTES_DF.format(x.endTime)
          val minuteEndList = minuteExecutorMap.getOrElse(endMinute, (new mutable.ListBuffer[ExecutorTimeSpan](), new mutable.ListBuffer[ExecutorTimeSpan]()))
          minuteEndList._2 += x
        }
      })

    var currentCount = 0
    minuteExecutorMap.keys.toBuffer
      .sortWith( (a, b) => a < b)
      .foreach( x => {
        currentCount = currentCount  + minuteExecutorMap(x)._1.size -  minuteExecutorMap(x)._2.size
        out.println (s"At ${x} executors added ${minuteExecutorMap(x)._1.size} & removed  ${minuteExecutorMap(x)._2.size} currently available ${currentCount}")
      })

    out.println("\nDone printing executors timeline...\n============================\n")
    out.toString()
  }
}
