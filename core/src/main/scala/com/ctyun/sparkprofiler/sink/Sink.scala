package com.ctyun.sparkprofiler.sink

import java.util.Properties

import com.ctyun.sparkprofiler.core.common.AppContext
import com.ctyun.sparkprofiler.sink.SinkMethod.SinkMethod
import com.ctyun.sparkprofiler.sink.method.console.ConsoleSink
import com.ctyun.sparkprofiler.sink.domain.{SimpleAppSinkInfo, SimpleJobSinkInfo, SimpleStageSinkInfo}
import com.ctyun.sparkprofiler.sink.method.mysql.MysqlSink


abstract class Sink{
  def sinkSimpleApp(simpleAppSinkInfo: SimpleAppSinkInfo,
                    simpleJobSinkInfo: Iterable[SimpleJobSinkInfo],
                    simpleStageSinkInfo: Iterable[SimpleStageSinkInfo]
                   ):Unit
}

object Sink {
  private val SINK_PROP_FILE = "sink.properties"
  private val ITEM_SINK_METHOD = "sink.method"
  private val ITEM_SINK_METHOD_MYSQL_HOST = "sink.Mysql.host"
  private val ITEM_SINK_METHOD_MYSQL_PORT = "sink.Mysql.port"
  private val ITEM_SINK_METHOD_MYSQL_USER = "sink.Mysql.user"
  private val ITEM_SINK_METHOD_MYSQL_PASSWORD = "sink.Mysql.password"
  private val ITEM_SINK_METHOD_MYSQL_DB = "sink.Mysql.db"


  val properties = new Properties()
  val in = this.getClass.getClassLoader.getResourceAsStream(SINK_PROP_FILE)
  properties.load(in)


  def getSink():Sink={
    getSinkMethod() match {
      case SinkMethod.Console=>{
        new ConsoleSink
      }
      case SinkMethod.Mysql=>{
        val instance = MysqlSink.getInstance(
          properties.getProperty(ITEM_SINK_METHOD_MYSQL_HOST),
          properties.getProperty(ITEM_SINK_METHOD_MYSQL_PORT),
          properties.getProperty(ITEM_SINK_METHOD_MYSQL_USER),
          properties.getProperty(ITEM_SINK_METHOD_MYSQL_PASSWORD),
          properties.getProperty(ITEM_SINK_METHOD_MYSQL_DB)
        )

        instance
      }
      case _ => {throw new IllegalArgumentException("Sink not supported yet")}
    }
  }

  private def getSinkMethod():SinkMethod={
    SinkMethod withName properties.getProperty(ITEM_SINK_METHOD)
  }
}

object SinkMethod extends Enumeration {
  type SinkMethod = Value
  val Console, Mysql = Value
}
