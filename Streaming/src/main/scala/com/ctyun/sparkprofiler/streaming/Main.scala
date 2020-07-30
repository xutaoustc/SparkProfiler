package com.ctyun.sparkprofiler.streaming

import com.ctyun.sparkprofiler.core
import com.ctyun.sparkprofiler.core.Main
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args:Array[String]):Unit={
    check(args)

    val ssc = if( StringUtils.isNotBlank( System.getProperty("master"))){
                val conf = new SparkConf().setAppName("SparkProfiler").setMaster(System.getProperty("master"))
                new StreamingContext(conf, Seconds(60))
              }else{
                val conf = new SparkConf().setAppName("SparkProfiler")
                new StreamingContext(conf, Seconds(60))
              }

    val path = args(0)
    val latestTime = args(1)
    val stream = ssc.receiverStream(new CustomDirectoryMonitorReceiver(path, latestTime.toLong))
        .repartition(256)

    stream.foreachRDD(rdd=>{
      rdd.foreach(eachFile=>
        try{
          core.Main.main(Array(eachFile))
        }catch{
          case ex: Exception=>{
            println(s"An error happened during ${eachFile}, reason: ${ex}")
          }
        }
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }

  private def check(args:Array[String]):Unit={
    if(args.length == 0)
      throw new IllegalArgumentException(INFO)
  }

  private val INFO =
    """
      | 第一个参数应该为要监控的SparkHistory目录地址
    """.stripMargin
}
