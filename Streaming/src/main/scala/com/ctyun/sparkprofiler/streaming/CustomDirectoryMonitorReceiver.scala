package com.ctyun.sparkprofiler.streaming

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomDirectoryMonitorReceiver(path:String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  private lazy val fs = new Path(path).getFileSystem(new Configuration())

  private lazy val loader = new CacheLoader[FileStatus,EventLogFileReader] {
    override def load(entry: FileStatus) = {
      EventLogFileReader(fs, entry).get
    }
  }

  private lazy val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(6,TimeUnit.HOURS)
    .build(loader)

  def onStart() {
    new Thread("receiver") {
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
          {
            x.printStackTrace()
            i+=1
          }
        }
      }
    }
  }


  private def receive() {
    while(true){
      doWithRetry {
        val completed = Option(fs.listStatus(new Path(path))).map(_.toSeq).getOrElse(Nil)
          .flatMap( status => {
            if(!cache.asMap().containsKey(status)){
              val reader = EventLogFileReader(fs, status)
              reader.flatMap( reader => Option((status,reader)) )
            }else Nil
          })
          .filter( _._2.completed )

        completed.foreach{ case (status, reader)=>{
          cache.put(status, reader)
        }}

        completed.map{ case (_, reader)=> reader.rootPath.toString}.foreach(store)
      }

      Thread.sleep(30000)
    }
  }
}