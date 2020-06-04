package com.ctyun.sparkprofiler.analyzer

import com.ctyun.sparkprofiler.common.AppContext

import scala.collection.mutable


class SimpleAppAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    out.println("\nPrinting application meterics. These metrics are collected at " +
      "task-level granularity and aggregated across the app (all tasks, stages, and jobs).\n")
    ac.appMetrics.print("Application Metrics", out)
    out.println("\n")
    out.toString()
  }
}
