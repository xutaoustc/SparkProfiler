package com.ctyun.sparkprofiler.sink.method.console

import com.ctyun.sparkprofiler.sink.Sink
import com.ctyun.sparkprofiler.sink.domain.SimpleAppSinkInfo

class ConsoleSink extends Sink{
  override def sinkSimpleApp(simpleAppSinkInfo:SimpleAppSinkInfo): Unit = {
    println(
      s"""
         |Task指标汇总 ,applicationID:${simpleAppSinkInfo.applicationID},appName:${simpleAppSinkInfo.appName}, task总数:${simpleAppSinkInfo.taskCount}
         |启动时间: ${simpleAppSinkInfo.startTime}, 结束时间: ${simpleAppSinkInfo.endTime}
       """.stripMargin)

  }
}
