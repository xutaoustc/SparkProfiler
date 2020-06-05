package com.ctyun.sparkprofiler.core.common

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

class AggregateValue {
  var value:    Long   = 0L
  var min:      Long   = Long.MaxValue
  var max:      Long   = Long.MinValue
  var mean:     Double = 0.0
  var variance: Double = 0.0
  var m2:       Double = 0.0

  override def toString(): String = {
    s"""{
       | "value": ${value},
       | "min": ${min},
       | "max": ${max},
       | "mean": ${mean},
       | "m2": ${m2}
       | "variance": ${variance}
       }""".stripMargin
  }

  def getMap(): Map[String, Any] = {
    Map("value" -> value,
      "min" -> min,
      "max" -> max,
      "mean" -> mean,
      "m2" -> m2,
      "variance" -> variance)
  }
}

object AggregateValue {
  def getValue(json: JValue): AggregateValue = {
    implicit val formats = DefaultFormats

    val value = new AggregateValue
    value.value = (json  \ "value").extract[Long]
    value.min = (json \ "min").extract[Long]
    value.max = (json \ "max").extract[Long]
    value.mean = (json \ "mean").extract[Double]
    value.variance = (json \ "variance").extract[Double]
    value.m2 = (json \ "m2").extractOrElse[Double](0.0)
    value
  }
}
