package com.ctyun.sparkprofiler.sink.method.console

import com.ctyun.sparkprofiler.sink.Sink
import com.ctyun.sparkprofiler.sink.domain.{SimpleAppSinkInfo, SimpleJobSinkInfo, SimpleStageSinkInfo}

class ConsoleSink extends Sink{
  override def sinkSimpleApp(simpleAppSinkInfo:SimpleAppSinkInfo,
                             simpleJobSinkInfo: Iterable[SimpleJobSinkInfo],
                             simpleStageSinkInfo: Iterable[SimpleStageSinkInfo]): Unit = {
    println(
      s"""
         |Task指标汇总 ,applicationID:${simpleAppSinkInfo.applicationID},appName:${simpleAppSinkInfo.appName}, sparkUser:${simpleAppSinkInfo.sparkUser}
         |task总数:${simpleAppSinkInfo.taskCount},taskDuration:${simpleAppSinkInfo.taskDuration},executorRuntime:${simpleAppSinkInfo.executorRuntime} 启动时间: ${simpleAppSinkInfo.startTime}, 结束时间: ${simpleAppSinkInfo.endTime}
       """.stripMargin)

    simpleJobSinkInfo.foreach(println(_))
    simpleStageSinkInfo.foreach(println(_))

  }
}
