package com.ctyun.sparkprofiler.core

object Main {

  def main(args:Array[String]):Unit={
    checkArgs(args)
    new EventHistoryReporter(args(0))
  }

  private def checkArgs(args:Array[String]): Unit ={
    val usage =
      """
        | Need to specify spark log file location
      """.stripMargin

    if(args.length < 1)
      throw new IllegalArgumentException(usage)
  }
}
