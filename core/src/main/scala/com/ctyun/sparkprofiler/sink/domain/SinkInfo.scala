package com.ctyun.sparkprofiler.sink.domain

case class SimpleAppSinkInfo(
                            applicationID: String,
                            appName: String,
                            sparkUser: String,
                            taskCount:Long,
                            taskDuration:Long,
                            executorRuntime:Long,
                            jvmGCTime:Long,
                            startTime:Long,
                            endTime:Long
                            )

case class SimpleJobSinkInfo(
                              applicationID: String,
                              jobID: Int,
                              taskCount:Long,
                              taskDuration:Long,
                              executorRuntime:Long,
                              jvmGCTime:Long,
                              startTime:Long,
                              endTime:Long
                            )

case class SimpleStageSinkInfo(
                              applicationID: String,
                              jobID: Int,
                              stageID: Int,
                              taskCount:Long,
                              taskDuration:Long,
                              executorRuntime:Long,
                              jvmGCTime:Long,
                              startTime:Long,
                              endTime:Long
                            )