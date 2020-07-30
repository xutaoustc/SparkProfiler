package com.ctyun.sparkprofiler.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomDirectoryMonitorReceiver(path:String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  private lazy val fs = new Path(path).getFileSystem(new Configuration())
  private var latestTime = 0l
  private var latestFileName:String = _


  def onStart() {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }


  def doWithRetry(fun: =>Unit): Unit ={
    var i = 1
    while(true){
      try{
        fun
        return
      }catch{
        case x:Exception => {
          if(i==3)
            throw x
          else
            i+=1
        }
      }
    }
  }


  private def receive() {
    while(true){
      doWithRetry {
        val completed = Option(fs.listStatus(new Path(path))).map(_.toSeq).getOrElse(Nil)
          .flatMap { entry => EventLogFileReader(fs, entry) }
          .filter( _.completed )

        if( latestTime == 0){
          completed.map(_.rootPath.toString).foreach(store)
        }else{
          completed.filter(f=>f.modificationTime>=latestTime && f.rootPath.toString!=latestFileName ).map(_.rootPath.toString).foreach(store)
        }


        if(completed.size != 0){
          val latestLog = completed.maxBy(_.modificationTime)
          latestTime = latestLog.modificationTime
          latestFileName = latestLog.rootPath.toString
        }
      }

      Thread.sleep(10000)
    }
  }
}