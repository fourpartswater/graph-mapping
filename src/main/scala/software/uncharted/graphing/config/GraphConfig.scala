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


import java.io.File

import software.uncharted.graphing.salt.ArcTypes

import scala.collection.JavaConverters._
import com.typesafe.config.{ConfigFactory, ConfigException, Config}
import grizzled.slf4j.{Logger, Logging}
import software.uncharted.graphing.analytics.CustomGraphAnalytic

import scala.util.Try


/**
  * Parse graph information from a configuration object
  */
object GraphConfig extends Logging {
  val graphKey = "graph"
  val analyticKey = "analytics"
  val levelsKey = "levels"
  val edgeKey = "edges"
  val edgeTypeKey = "type"
  val formatKey = "format"
  val formatTypeKey = "type"
  val minLengthKey = "min"
  val maxLengthKey = "max"

  def apply(config: Config): Option[GraphConfig] = {
    try {
      val graphConfig = config.getConfig(graphKey)
      val analytics = graphConfig.getStringList(analyticKey).asScala.map { analyticName =>
        CustomGraphAnalytic(analyticName)
      }
      val levels = graphConfig.getIntList(levelsKey).asScala.map(_.intValue())
      val edgeConfig = graphConfig.getConfig(edgeKey)
      val edgeType = if (edgeConfig.hasPath(edgeTypeKey)) {
        edgeConfig.getString(edgeTypeKey).toLowerCase.trim match {
          case "inter" => Some(1)
          case "intra" => Some(0)
          case et => throw new IllegalArgumentException("Illegal edge type " + et)
        }
      } else None
      val formatConfig = edgeConfig.getConfig(formatKey)
      val formatType = formatConfig.getString(formatTypeKey).toLowerCase.trim match {
        case "leaderline" => ArcTypes.LeaderLine
        case "line" => ArcTypes.FullLine
        case "leaderarc" => ArcTypes.LeaderArc
        case "arc" => ArcTypes.FullArc
        case lt => throw new IllegalArgumentException("Illegal line type " + lt)
      }
      val minSegLength = if (formatConfig.hasPath(minLengthKey)) Some(formatConfig.getInt(minLengthKey)) else None
      val maxSegLength = if (formatConfig.hasPath(maxLengthKey)) Some(formatConfig.getInt(maxLengthKey)) else None

      Some(GraphConfig(analytics, levels, edgeType, formatType, minSegLength, maxSegLength))
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$graphKey]", e)
        None
    }
  }

  /**
    * Get the full configuration object associated with graph tiling jobs
    *
    * @param arguments The command-line arguments with which the graph tiling job was run.  Config files specified
    *                  earlier take precedence over config files specified later.
    * @return A full configuration, with default fallbacks and environmental overrides
    */
  def getFullConfiguration(arguments: Array[String], logger: Logger): Config = {
    // Read in and merge command-line specified configuration files
    val specifiedConfigs = arguments.map{arg =>
      val file = new File(arg)
      if (file.exists()) {
        Try(ConfigFactory.parseReader(scala.io.Source.fromFile(file).bufferedReader()).resolve()).toOption
      } else {
        None
      }
    }.filter(_.isDefined).map(_.get)

    // Get the environment-based config
    val environmentConfig = ConfigFactory.load()

    // Get our fallback config file
    val defaultConfig = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/graph-defaults.conf")).bufferedReader()).resolve()

    // Merge these together
    (specifiedConfigs :+ defaultConfig).fold(environmentConfig)((base, fallback) => base.withFallback(fallback))
  }
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
