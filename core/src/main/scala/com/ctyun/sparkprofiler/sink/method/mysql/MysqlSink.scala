package com.ctyun.sparkprofiler.sink.method.mysql

import com.ctyun.sparkprofiler.sink.Sink
import com.ctyun.sparkprofiler.sink.domain.{SimpleAppSinkInfo, SimpleJobSinkInfo, SimpleStageSinkInfo}
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.commons.dbutils.QueryRunner

class MysqlSink() extends Sink{
  var ds: ComboPooledDataSource = _

  override def sinkSimpleApp(simpleAppSinkInfo:SimpleAppSinkInfo,
                             simpleJobSinkInfo: Iterable[SimpleJobSinkInfo],
                             simpleStageSinkInfo: Iterable[SimpleStageSinkInfo]): Unit = {
    val qr = new QueryRunner(ds);

    try{
      val applicationSQL="replace into application_overview(appId,appName,sparkUser,taskCount,startTime,endTime) values(?,?,?,?,?,?)"
      qr.update(applicationSQL,
        simpleAppSinkInfo.applicationID,
        simpleAppSinkInfo.appName,
        simpleAppSinkInfo.sparkUser,
        simpleAppSinkInfo.taskCount.toString,
        simpleAppSinkInfo.startTime.toString,
        simpleAppSinkInfo.endTime.toString)

      val jobSQL="replace into job_overview(appId,jobID,taskCount,startTime,endTime) values(?,?,?,?,?)"
      simpleJobSinkInfo.foreach(job=>{
        qr.update(jobSQL,
          job.applicationID,
          job.jobID.toString,
          job.taskCount.toString,
          job.startTime.toString,
          job.endTime.toString)
      })

      val stageSQL="replace into stage_overview(appId,jobID,stageID,taskCount,startTime,endTime) values(?,?,?,?,?,?)"
      simpleStageSinkInfo.foreach(stage=>{
        qr.update(stageSQL,
          stage.applicationID,
          stage.jobID.toString,
          stage.stageID.toString,
          stage.taskCount.toString,
          stage.startTime.toString,
          stage.endTime.toString)
      })
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
