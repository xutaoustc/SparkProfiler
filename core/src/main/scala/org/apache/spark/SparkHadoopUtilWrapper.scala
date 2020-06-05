package org.apache.spark

import org.apache.spark.deploy.SparkHadoopUtil

object SparkHadoopUtilWrapper {
  val sparkHadoopUtil = SparkHadoopUtil.get
}
