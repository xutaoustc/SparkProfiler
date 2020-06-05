package com.ctyun.sparkprofiler.core.analyzer

import java.util.Date
import java.util.concurrent.TimeUnit

import com.ctyun.sparkprofiler.core.common.AppContext

import scala.collection.mutable.ListBuffer

trait AppAnalyzer {
  def analyze(ac: AppContext): String = {
    analyze(ac, ac.appInfo.startTime, ac.appInfo.endTime)
  }

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String

  import java.text.SimpleDateFormat
  val DF = new SimpleDateFormat("hh:mm:ss:SSS")
  val MINUTES_DF = new SimpleDateFormat("hh:mm")

  /*
  print time
   */
  def pt(x: Long) : String = {
    DF.format(new  Date(x))
  }
  /*
  print duration
   */
  def pd(millis: Long) : String = {
    "%02dm %02ds".format(
      TimeUnit.MILLISECONDS.toMinutes(millis),
      TimeUnit.MILLISECONDS.toSeconds(millis) -
        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
    )
  }

  def pcm(millis: Long) : String = {
    val millisForMinutes = millis % (60*60*1000)

    "%02dh %02dm".format(
      TimeUnit.MILLISECONDS.toHours(millis),
      TimeUnit.MILLISECONDS.toMinutes(millisForMinutes))
  }

  implicit class PrintlnStringBuilder(sb: StringBuilder) {
    def println(x: Any): StringBuilder = {
      sb.append(x).append("\n")
    }
    def print(x: Any): StringBuilder = {
      sb.append(x)
    }
  }
}

object AppAnalyzer {
  def startAnalyzers(appContext: AppContext): Unit = {
    val anlyzList = new ListBuffer[AppAnalyzer]
    anlyzList += new SimpleAppAnalyzer
//    anlyzList += new HostTimelineAnalyzer
//    list += new ExecutorTimelineAnalyzer
//    list += new AppTimelineAnalyzer
//    list += new JobOverlapAnalyzer
//    list += new EfficiencyStatisticsAnalyzer
//    list += new ExecutorWallclockAnalyzer   TODO
//    list += new StageSkewAnalyzer


    anlyzList.foreach( anlyz => {
      try {
        val output = anlyz.analyze(appContext)
        println(output)
      } catch {
        case e:Throwable => {
          println(s"Failed in Analyzer ${anlyz.getClass.getSimpleName}")
          e.printStackTrace()
        }
      }
    })
  }

}
