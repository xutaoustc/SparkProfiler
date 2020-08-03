package com.ctyun.sparkprofiler.sink.domain

import com.ctyun.sparkprofiler.core.util.Tabulator

case class SimpleAppSinkInfo(
                            applicationID: String,
                            appName: String,
                            sparkUser: String,
                            taskCount:Long,
                            taskDuration:Long,
                            executorRuntime:Long,
                            jvmGCTime:Long,
                            memoryBytesSpilled:Long,
                            diskBytesSpilled:Long,
                            peakExecutionMemory:Long,
                            inputBytesRead:Long,
                            outputBytesWritten:Long,
                            resultSize:Long,
                            shuffleWriteBytesWritten:Long,
                            shuffleWriteRecordsWritten:Long,
                            shuffleWriteTime:Long,
                            shuffleReadFetchWaitTime:Long,
                            shuffleReadBytesRead:Long,
                            shuffleReadRecordsRead:Long,
                            shuffleReadLocalBlocks:Long,
                            shuffleReadRemoteBlocks:Long,
                            startTime:Long,
                            endTime:Long
                            ){
  override def toString = Tabulator.format(
                            List(
                              classOf[SimpleAppSinkInfo].getDeclaredFields.map(_.getName).toList,
                              this.productIterator.toList
                            )
                          )
}

case class SimpleJobSinkInfo(
                              applicationID: String,
                              jobID: Int,
                              taskCount:Long,
                              taskDuration:Long,
                              executorRuntime:Long,
                              jvmGCTime:Long,
                              memoryBytesSpilled:Long,
                              diskBytesSpilled:Long,
                              peakExecutionMemory:Long,
                              inputBytesRead:Long,
                              outputBytesWritten:Long,
                              resultSize:Long,
                              shuffleWriteBytesWritten:Long,
                              shuffleWriteRecordsWritten:Long,
                              shuffleWriteTime:Long,
                              shuffleReadFetchWaitTime:Long,
                              shuffleReadBytesRead:Long,
                              shuffleReadRecordsRead:Long,
                              shuffleReadLocalBlocks:Long,
                              shuffleReadRemoteBlocks:Long,
                              startTime:Long,
                              endTime:Long
                            ){
  override def toString = Tabulator.format(
                            List(
                              classOf[SimpleJobSinkInfo].getDeclaredFields.map(_.getName).toList,
                              this.productIterator.toList
                            )
                          )
}

case class SimpleStageSinkInfo(
                              applicationID: String,
                              jobID: Int,
                              stageID: Int,
                              taskCount:Long,
                              taskDuration:Long,
                              executorRuntime:Long,
                              jvmGCTime:Long,
                              memoryBytesSpilled:Long,
                              diskBytesSpilled:Long,
                              peakExecutionMemory:Long,
                              inputBytesRead:Long,
                              outputBytesWritten:Long,
                              resultSize:Long,
                              shuffleWriteBytesWritten:Long,
                              shuffleWriteRecordsWritten:Long,
                              shuffleWriteTime:Long,
                              shuffleReadFetchWaitTime:Long,
                              shuffleReadBytesRead:Long,
                              shuffleReadRecordsRead:Long,
                              shuffleReadLocalBlocks:Long,
                              shuffleReadRemoteBlocks:Long,
                              startTime:Long,
                              endTime:Long
                            ){
  override def toString = Tabulator.format(
                            List(
                              classOf[SimpleStageSinkInfo].getDeclaredFields.map(_.getName).toList,
                              this.productIterator.toList
                            )
                          )
}

