package com.ctyun.sparkprofiler.sink.method.console

import com.ctyun.sparkprofiler.sink.Sink
import com.ctyun.sparkprofiler.sink.domain.{SimpleAppSinkInfo, SimpleJobSinkInfo, SimpleStageSinkInfo}

class ConsoleSink extends Sink{
  override def sinkSimpleApp(simpleAppSinkInfo:SimpleAppSinkInfo,
                             simpleJobSinkInfo: Iterable[SimpleJobSinkInfo],
                             simpleStageSinkInfo: Iterable[SimpleStageSinkInfo]): Unit = {
    println("Application Info")
    Option(simpleAppSinkInfo).foreach(println)
    println("Job Info")
    simpleJobSinkInfo.foreach(println(_))
    println("Stage Info")
    simpleStageSinkInfo.foreach(println(_))
  }
}
