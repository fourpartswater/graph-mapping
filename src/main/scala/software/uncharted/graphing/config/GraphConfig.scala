package software.uncharted.graphing.config


import software.uncharted.graphing.salt.ArcTypes

import scala.collection.JavaConverters._
import com.typesafe.config.{ConfigFactory, ConfigException, Config}
import grizzled.slf4j.{Logger, Logging}
import software.uncharted.graphing.analytics.CustomGraphAnalytic


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
    * @param arguments the command-line arguments with which the graph tiling job was run
    * @return A full configuration, with default fallbacks and environmental overrides
    */
  def getFullConfiguration(arguments: Array[String], logger: Logger): Config = {
    // get the properties file path
    if (arguments.length < 1) {
      logger.error("Path to configuration file required")
      sys.exit(-1)
    }

    // Get the environment-based config
    val environmentConfig = ConfigFactory.load()

    // Get command-line specified config files
    val clConfig = ConfigFactory.parseReader(scala.io.Source.fromFile(arguments(0)).bufferedReader()).resolve()

    // Get our fallback config file
    val defaultConfig = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/graph-defaults.conf")).bufferedReader()).resolve()

    // Merge these together
    environmentConfig.withFallback(clConfig).withFallback(defaultConfig)
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
