package com.ctyun.sparkprofiler.core.common

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
