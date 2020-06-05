package com.ctyun.sparkprofiler.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
        val allFile = fs.listStatus(new Path(path)).filter(path => !path.getPath.toString.contains(".inprogress") && !path.isDirectory )

        if( latestTime == 0){
          allFile.map(_.getPath.toString).foreach(store)
        }else{
          allFile.filter(f=>f.getModificationTime>=latestTime && f.getPath.toString!=latestFileName ).map(_.getPath.toString).foreach(store)
        }

        if(allFile.size != 0){
          val latestFile = allFile.maxBy(_.getModificationTime)
          latestTime = latestFile.getModificationTime
          latestFileName = latestFile.getPath.toString
        }
      }

      Thread.sleep(10000)
    }
  }
}