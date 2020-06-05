package com.ctyun.sparkprofiler.core.analyzer

import com.ctyun.sparkprofiler.core.common.AppContext

import scala.collection.mutable


class SimpleAppAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    ac.appMetrics.print("Task指标汇总", out)
    out.toString()
  }
}
