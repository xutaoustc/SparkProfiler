package com.ctyun.sparkprofiler.sink.domain

case class SimpleAppSinkInfo(
                            applicationID: String,
                            appName: String,
                            taskCount:Long,
                            startTime:Long,
                            endTime:Long
                            )
