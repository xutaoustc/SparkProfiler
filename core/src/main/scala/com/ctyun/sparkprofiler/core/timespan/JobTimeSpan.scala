package com.ctyun.sparkprofiler.core.timespan

import com.ctyun.sparkprofiler.core.common.{AggregateMetrics, AppContext}
import org.json4s.DefaultFormats

import scala.collection.mutable

class JobTimeSpan(val jobID: Long) extends TimeSpan {
  var stageMap = new mutable.HashMap[Int, StageTimeSpan]()

  def addStage(stage: StageTimeSpan): Unit = {
    stageMap (stage.stageID) = stage
  }

  override def getMap(): Map[String, _ <: Any] = {
    implicit val formats = DefaultFormats

    Map(
      "jobID" -> jobID,
      "jobMetrics" -> metrics.getMap,
      "stageMap" -> AppContext.getMap(stageMap)) ++ super.getStartEndTime()
  }


  def computeCriticalTimeForJob(): Long = {
    if (stageMap.isEmpty) {
      0L
    }else {
      val maxStageID = stageMap.map(x => x._1).max
      val data = stageMap.map(x =>
        (x._1,
          (
            x._2.parentStageIDs,
            x._2.metrics.map(AggregateMetrics.executorRuntime).max
          )
        )
      )
      criticalTime(maxStageID, data)
    }
  }

  /*
  recursive function to compute critical time starting from the last stage
   */
  private def criticalTime(stageID: Int, data: mutable.HashMap[Int, (Seq[Int], Long)]): Long = {
    //Provide 0 value for
    val stageData = data.getOrElse(stageID, (List.empty[Int], 0L))
    stageData._2 + {
      if (stageData._1.size == 0) {
        0L
      }else {
        stageData._1.map(x => criticalTime(x, data)).max
      }
    }
  }

}

