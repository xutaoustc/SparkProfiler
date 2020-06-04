package com.ctyun.sparkprofiler.common


import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

case class ApplicationInfo (var applicationID:String = "NA",
                            var startTime:Long = 0L,
                            var endTime:Long = 0L) {

  def getMap(): Map[String, Any] = {
    implicit val formats = DefaultFormats
    Map("applicationID" -> applicationID, "startTime" -> startTime, "endTime" -> endTime)
  }
}

object ApplicationInfo {

  def getObject(jvalue: JValue): ApplicationInfo = {
    implicit val formats = DefaultFormats

    ApplicationInfo(
      (jvalue \ "applicationID").extract[String],
      (jvalue \ "startTime").extract[Long],
      (jvalue \ "endTime").extract[Long])
  }
}
