/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.config


import software.uncharted.xdata.ops.salt.ArcTypes

import scala.collection.JavaConverters._ //scalastyle:ignore
import com.typesafe.config.{Config}
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.util.Try


/**
  * Parse graph information from a configuration object
  */
object GraphConfig extends ConfigParser {
  val graphKey = "graph"
  val analyticKey = "analytics"
  val levelsKey = "levels"
  val edgeKey = "edges"
  val edgeTypeKey = "type"
  val formatKey = "format"
  val formatTypeKey = "type"
  val minLengthKey = "min"
  val maxLengthKey = "max"

  //scalastyle:off cyclomatic.complexity
  def parse(config: Config): Try[GraphConfig] = {
    Try {
      val graphConfig = config.getConfig(graphKey)
      val analytics = if (graphConfig.hasPath(analyticKey)) {
        graphConfig.getStringList(analyticKey).asScala.map { analyticName =>
          CustomGraphAnalytic(analyticName, "")
        }
      } else {
        Seq()
      }

      val levels = graphConfig.getIntList(levelsKey).asScala.map(_.intValue())
      val edgeConfig = graphConfig.getConfig(edgeKey)
      val edgeType = if (edgeConfig.hasPath(edgeTypeKey)) {
        edgeConfig.getString(edgeTypeKey).toLowerCase.trim match {
          case "inter" => Some(1)
          case "intra" => Some(0)
          case et: Any => throw new IllegalArgumentException("Illegal edge type " + et)
        }
      } else {
        None
      }

      val formatConfig = edgeConfig.getConfig(formatKey)
      val formatType = formatConfig.getString(formatTypeKey).toLowerCase.trim match {
        case "leaderline" => ArcTypes.LeaderLine
        case "line" => ArcTypes.FullLine
        case "leaderarc" => ArcTypes.LeaderArc
        case "arc" => ArcTypes.FullArc
        case lt: Any => throw new IllegalArgumentException("Illegal line type " + lt)
      }
      val minSegLength = if (formatConfig.hasPath(minLengthKey)) Some(formatConfig.getInt(minLengthKey)) else None
      val maxSegLength = if (formatConfig.hasPath(maxLengthKey)) Some(formatConfig.getInt(maxLengthKey)) else None

      GraphConfig(analytics, levels, edgeType, formatType, minSegLength, maxSegLength)
    }
  }
  //scalastyle:on cyclomatic.complexity
}

case class GraphConfig(analytics: Seq[CustomGraphAnalytic[_]],
                       levelsPerHierarchyLevel: Seq[Int],
                       edgeType: Option[Int],
                       formatType: ArcTypes.Value,
                       minSegLength: Option[Int],
                       maxSegLength: Option[Int]) {
  // Get the tiling levels corresponding to each hierarchy level
  val graphLevelsByHierarchyLevel = {
    levelsPerHierarchyLevel.scanLeft(0)(_ + _).sliding(2).map(bounds =>
      (bounds(0), bounds(1) - 1)
    ).toList.reverse.zipWithIndex.reverse
  }
}
