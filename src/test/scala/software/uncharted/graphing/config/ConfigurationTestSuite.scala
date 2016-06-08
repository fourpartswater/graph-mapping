package software.uncharted.graphing.config

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import software.uncharted.graphing.salt.ArcTypes


class ConfigurationTestSuite extends FunSuite {
  private def withKeys (overrides: Map[String, String])(test: => Unit) = {
    val oldValues = overrides.map { case (key, value) =>
      val oldValue = if (sys.props.contains(key)) Some(sys.props(key)) else None
      sys.props(key) = value
      key -> oldValue
    }
    try {
      println(sys.props)
      test
    } finally {
      oldValues.map{case (key, valueOpt) =>
          if (valueOpt.isDefined) {
            sys.props(key) = valueOpt.get
          } else {
            sys.props.remove(key)
          }
      }
    }
  }

  private def getGraphConfig = {
    ConfigFactory.invalidateCaches()
    val envConfig = ConfigFactory.load()
    val defaultConfig = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/graph-defaults.conf")).bufferedReader()).resolve()
    val config = envConfig.withFallback(defaultConfig)
    GraphConfig(config).get
  }

  test("Test minimal override") {
    withKeys(Map("graph.levels.0" -> "2")) {
      val graphConfig = getGraphConfig

      assert(0 === graphConfig.analytics.length)
      assert(ArcTypes.LeaderLine === graphConfig.formatType)
      assert(graphConfig.edgeType.isEmpty)
      assert(graphConfig.maxSegLength.isEmpty)
      assert(graphConfig.minSegLength.isEmpty)
    }
  }

  test("Test edge type override") {
    withKeys(Map("graph.edges.type" -> "intra", "graph.levels.0" -> "3")) {
      val graphConfig = getGraphConfig

      assert(Some(0) === graphConfig.edgeType)
    }

    withKeys(Map("graph.edges.type" -> "inter", "graph.levels.0" -> "4")) {
      val graphConfig = getGraphConfig

      assert(Some(1) === graphConfig.edgeType)
    }
  }

  test("Test format type override") {
    withKeys(Map("graph.edges.format.type" -> "leaderarc", "graph.levels.0" -> "5")) {
      val graphConfig = getGraphConfig

      assert(ArcTypes.LeaderArc == graphConfig.formatType)
    }
  }

  test("Test minimum segment length override") {
    withKeys(Map("graph.edges.format.min" -> "4", "graph.edges.format.max" -> "1024", "graph.levels.0" -> "6")) {
      val graphConfig = getGraphConfig

      assert(Some(4) === graphConfig.minSegLength)
      assert(Some(1024) === graphConfig.maxSegLength)
    }
  }

  test("Test hierarchy level override") {
    withKeys(Map("graph.levels.0" -> "3", "graph.levels.1" -> "2", "graph.levels.2" -> "4", "graph.levels.3" -> "1")) {
      val graphConfig = getGraphConfig

      assert(List(((0, 2), 3), ((3, 4), 2), ((5, 8), 1), ((9, 9), 0)) === graphConfig.graphLevelsByHierarchyLevel)
    }
  }
}
