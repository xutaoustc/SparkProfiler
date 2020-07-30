package org.apache.spark

import java.io.InputStream

import org.apache.spark.scheduler.SparkListenerInterface

class ReplayListenerBusWrapper {
  private val bus = new org.apache.spark.scheduler.ReplayListenerBus()

  def addListener(listener:SparkListenerInterface):Unit={
    bus.addListener(listener)
  }

  def replay(logData: InputStream,
             sourceName: String,
             maybeTruncated: Boolean = false):Boolean  ={
    bus.replay(logData,sourceName, maybeTruncated)
  }
}
