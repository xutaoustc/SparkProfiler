package com.ctyun.sparkprofiler.core

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import com.ning.compress.lzf.LZFInputStream
import net.jpountz.lz4.LZ4BlockInputStream
import org.apache.hadoop.conf.Configuration
import org.xerial.snappy.SnappyInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ReplayListenerBusWrapper


class EventHistoryReporter(file: String) {
  private val hadoopConf = new Configuration()

  val bus = new ReplayListenerBusWrapper
  bus.addListener( new EventHistoryListener() )
  bus.replay(getDecodedInputStream(file), file, boolean2Boolean(false))

  private def getDecodedInputStream(file: String): InputStream = {
    val fs = FileSystem.get(new URI(file), hadoopConf)
    val path = new Path(file)
    val bufStream = new BufferedInputStream(fs.open(path))

    val codecName: Option[String] = path.getName.stripSuffix(".inprogress").split("\\.").tail.lastOption
    codecName.getOrElse("") match {
      case "lz4" => new LZ4BlockInputStream(bufStream)
      case "lzf" => new LZFInputStream(bufStream)
      case "snappy" => new SnappyInputStream(bufStream)
      case _ => bufStream
    }
  }
}
