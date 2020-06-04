package com.ctyun.sparkprofiler.timespan
import org.json4s.DefaultFormats

class ExecutorTimeSpan(val executorID: String,
                       val hostID: String,
                       val cores: Int) extends TimeSpan {

  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats

    Map("executorID" -> executorID, "hostID" -> hostID, "cores" -> cores, "executorMetrics" -> metrics.getMap()) ++ super.getStartEndTime()
  }
}
