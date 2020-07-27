package com.ctyun.sparkprofiler.core.timespan

class ExecutorTimeSpan(val executorID: String,
                       val hostID: String,
                       val cores: Int) extends TimeSpan {
}
