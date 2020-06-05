package com.ctyun.sparkprofiler.core.timespan

import org.json4s.DefaultFormats


class HostTimeSpan(val hostID: String) extends TimeSpan {
  override def duration():Option[Long] = {
    Some(super.duration().getOrElse(System.currentTimeMillis() - startTime))
  }

  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats
    Map("hostID" -> hostID, "hostMetrics" -> metrics.getMap) ++ super.getStartEndTime()
  }

}