package com.ctyun.sparkprofiler.sink.domain

case class SimpleAppSinkInfo(
                            applicationID: String,
                            appName: String,
                            sparkUser: String,
                            taskCount:Long,
                            startTime:Long,
                            endTime:Long
                            )
