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
      val applicationSQL="replace into application_overview(appId,appName,sparkUser,taskCount,taskDuration,executorRuntime,jvmGCTime,memoryBytesSpilled,diskBytesSpilled,peakExecutionMemory,inputBytesRead,outputBytesWritten,resultSize,shuffleWriteBytesWritten,shuffleWriteRecordsWritten,shuffleWriteTime,shuffleReadFetchWaitTime,shuffleReadBytesRead,shuffleReadRecordsRead,shuffleReadLocalBlocks,shuffleReadRemoteBlocks,startTime,endTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      qr.update(applicationSQL,
        simpleAppSinkInfo.applicationID,
        simpleAppSinkInfo.appName,
        simpleAppSinkInfo.sparkUser,
        simpleAppSinkInfo.taskCount.toString,
        simpleAppSinkInfo.taskDuration.toString,
        simpleAppSinkInfo.executorRuntime.toString,
        simpleAppSinkInfo.jvmGCTime.toString,
        simpleAppSinkInfo.memoryBytesSpilled.toString,
        simpleAppSinkInfo.diskBytesSpilled.toString,
        simpleAppSinkInfo.peakExecutionMemory.toString,
        simpleAppSinkInfo.inputBytesRead.toString,
        simpleAppSinkInfo.outputBytesWritten.toString,
        simpleAppSinkInfo.resultSize.toString,
        simpleAppSinkInfo.shuffleWriteBytesWritten.toString,
        simpleAppSinkInfo.shuffleWriteRecordsWritten.toString,
        simpleAppSinkInfo.shuffleWriteTime.toString,
        simpleAppSinkInfo.shuffleReadFetchWaitTime.toString,
        simpleAppSinkInfo.shuffleReadBytesRead.toString,
        simpleAppSinkInfo.shuffleReadRecordsRead.toString,
        simpleAppSinkInfo.shuffleReadLocalBlocks.toString,
        simpleAppSinkInfo.shuffleReadRemoteBlocks.toString,
        simpleAppSinkInfo.startTime.toString,
        simpleAppSinkInfo.endTime.toString)

      val jobSQL="replace into job_overview(appId,jobID,taskCount,taskDuration,executorRuntime,jvmGCTime,memoryBytesSpilled,diskBytesSpilled,peakExecutionMemory,inputBytesRead,outputBytesWritten,resultSize,shuffleWriteBytesWritten,shuffleWriteRecordsWritten,shuffleWriteTime,shuffleReadFetchWaitTime,shuffleReadBytesRead,shuffleReadRecordsRead,shuffleReadLocalBlocks,shuffleReadRemoteBlocks,startTime,endTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      simpleJobSinkInfo.foreach(job=>{
        qr.update(jobSQL,
          job.applicationID,
          job.jobID.toString,
          job.taskCount.toString,
          job.taskDuration.toString,
          job.executorRuntime.toString,
          job.jvmGCTime.toString,
          job.memoryBytesSpilled.toString,
          job.diskBytesSpilled.toString,
          job.peakExecutionMemory.toString,
          job.inputBytesRead.toString,
          job.outputBytesWritten.toString,
          job.resultSize.toString,
          job.shuffleWriteBytesWritten.toString,
          job.shuffleWriteRecordsWritten.toString,
          job.shuffleWriteTime.toString,
          job.shuffleReadFetchWaitTime.toString,
          job.shuffleReadBytesRead.toString,
          job.shuffleReadRecordsRead.toString,
          job.shuffleReadLocalBlocks.toString,
          job.shuffleReadRemoteBlocks.toString,
          job.startTime.toString,
          job.endTime.toString)
      })

      val stageSQL="replace into stage_overview(appId,jobID,stageID,taskCount,taskDuration,executorRuntime,jvmGCTime,memoryBytesSpilled,diskBytesSpilled,peakExecutionMemory,inputBytesRead,outputBytesWritten,resultSize,shuffleWriteBytesWritten,shuffleWriteRecordsWritten,shuffleWriteTime,shuffleReadFetchWaitTime,shuffleReadBytesRead,shuffleReadRecordsRead,shuffleReadLocalBlocks,shuffleReadRemoteBlocks,startTime,endTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      simpleStageSinkInfo.foreach(stage=>{
        qr.update(stageSQL,
          stage.applicationID,
          stage.jobID.toString,
          stage.stageID.toString,
          stage.taskCount.toString,
          stage.taskDuration.toString,
          stage.executorRuntime.toString,
          stage.jvmGCTime.toString,
          stage.memoryBytesSpilled.toString,
          stage.diskBytesSpilled.toString,
          stage.peakExecutionMemory.toString,
          stage.inputBytesRead.toString,
          stage.outputBytesWritten.toString,
          stage.resultSize.toString,
          stage.shuffleWriteBytesWritten.toString,
          stage.shuffleWriteRecordsWritten.toString,
          stage.shuffleWriteTime.toString,
          stage.shuffleReadFetchWaitTime.toString,
          stage.shuffleReadBytesRead.toString,
          stage.shuffleReadRecordsRead.toString,
          stage.shuffleReadLocalBlocks.toString,
          stage.shuffleReadRemoteBlocks.toString,
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
