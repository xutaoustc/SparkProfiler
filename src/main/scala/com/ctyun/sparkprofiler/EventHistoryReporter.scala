package com.ctyun.sparkprofiler

import java.io.{BufferedInputStream, InputStream}
import java.net.URI

import com.ctyun.sparkprofiler.util.HDFSConfigHelper
import com.ning.compress.lzf.LZFInputStream
import net.jpountz.lz4.LZ4BlockInputStream
import org.xerial.snappy.SnappyInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ReplayListenerBusWrapper


class EventHistoryReporter(file: String) {
  val bus = new ReplayListenerBusWrapper
  bus.addListener( new EventHistoryListener() )
  bus.replay(getDecodedInputStream(file), file, boolean2Boolean(false))


  private def getDecodedInputStream(file: String): InputStream = {
    val fs = FileSystem.get(new URI(file), HDFSConfigHelper.getHadoopConf())
    val path = new Path(file)
    val bufStream = new BufferedInputStream(fs.open(path))

    val logName = path.getName.stripSuffix(".inprogress")
    val codecName: Option[String] = logName.split("\\.").tail.lastOption

    codecName.getOrElse("") match {
      case "lz4" => new LZ4BlockInputStream(bufStream)
      case "lzf" => new LZFInputStream(bufStream)
      case "snappy" => new SnappyInputStream(bufStream)
      case _ => bufStream
    }
  }
}
