package com.ctyun.sparkprofiler.core

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ReplayListenerBusWrapper
import org.apache.spark.deploy.history.EventLogFileReader


class EventHistoryReporter(rootPath: String) {
  private val hadoopConf = new Configuration()
  val fs = FileSystem.get(new URI(rootPath), hadoopConf)
  val reader = EventLogFileReader(fs, new Path(rootPath)).get

  val bus = new ReplayListenerBusWrapper
  bus.addListener( new EventHistoryListener() )

  val logFiles = reader.listEventLogFiles
  var continueReplay = true
  logFiles.foreach { file =>
    if (continueReplay) {
      val in = EventLogFileReader.openEventLog(file.getPath, fs)
      continueReplay = bus.replay(in, file.getPath.toString, boolean2Boolean(false))
    }
  }
}
