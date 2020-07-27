package com.ctyun.sparkprofiler.sink.method.mysql

import com.ctyun.sparkprofiler.sink.Sink
import com.ctyun.sparkprofiler.sink.domain.SimpleAppSinkInfo
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.commons.dbutils.QueryRunner

class MysqlSink() extends Sink{
  var ds: ComboPooledDataSource = _

  override def sinkSimpleApp(simpleAppSinkInfo:SimpleAppSinkInfo): Unit = {
    val qr = new QueryRunner(ds);
    val sql="replace into job_overview(appId,appName,taskCount,startTime,endTime) values(?,?,?,?,?)";
    try{
      qr.update(sql,
        simpleAppSinkInfo.applicationID,
        simpleAppSinkInfo.appName,
        simpleAppSinkInfo.taskCount.toString,
        simpleAppSinkInfo.startTime.toString,
        simpleAppSinkInfo.endTime.toString)
    }catch {
      case e:Exception=> e.printStackTrace();
    }
  }
}

object MysqlSink{
  private var instance:MysqlSink = null

  def getInstance(host:String, port:String, user:String ,password:String, db:String):MysqlSink={
    if(instance == null){
      MysqlSink.getClass synchronized{
        if(instance == null){
          instance = new MysqlSink()
          instance.ds = new ComboPooledDataSource;
          instance.ds.setDriverClass("com.mysql.cj.jdbc.Driver")
          instance.ds.setJdbcUrl(s"jdbc:mysql://${host}:${port}/${db}")
          instance.ds.setUser(user)
          instance.ds.setPassword(password)
        }
      }
    }
    instance
  }
}
