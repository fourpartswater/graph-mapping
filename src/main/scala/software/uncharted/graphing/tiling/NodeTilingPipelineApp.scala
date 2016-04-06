/**
  * Copyright © 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted™, formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.tiling

import java.io.FileInputStream
import java.util.{Date, Properties}

import com.oculusinfo.binning.io.serialization.DefaultTileSerializerFactoryProvider
import com.oculusinfo.tilegen.datasets.{ValueExtractorFactory, TilingTaskParameters}
import com.oculusinfo.tilegen.pipeline.PipelineOperations._
import com.oculusinfo.tilegen.pipeline._
import com.oculusinfo.tilegen.util.{PropertiesWrapper, KeyValueArgumentSource, ArgumentParser}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * A pipeline application to create heatmap tiles of the nodes in a graph.
 *
 * Basically, this is a slightly modified heatmap tiler.
 */
object NodeTilingPipelineApp {
  import GraphOperations._

  private def getKVFile (fileName: String):KeyValueArgumentSource = {
    val props = new Properties()
    val propsFile = new FileInputStream(fileName)
    props.load(propsFile)
    propsFile.close()
    new PropertiesWrapper(props)
  }

  def main (args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    val argParser = new ArgumentParser(args)

    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

    try {
      val base = argParser.getString("base", "The base location of graph layout information")
      val levels = argParser.getIntSeq("levels",
        """The number of levels per hierarchy level.  These will be applied in reverse order -
          | so that a value of "4,3,2" means that hierarchy level 2 will be used for tiling
          | levels 0-3, hierarcy level 1 will be used for tiling levels 4-6, and hierarchy
          | level 0 will be used for tiling levels 7 and 8.""".stripMargin)
      val nodeTest = argParser.getStringOption("nodeTest", "A regular expression that lines must match to be considered nodes")
      val nodeFileDescriptor = argParser.getString("nodeColumns", "A file containing parse information for the node lines")
      val tileSet = argParser.getString("name", "The name of the node tile set to produce")
      val tileDesc = argParser.getStringOption("desc", "A description of the node tile set to produce")
      val prefix = argParser.getStringOption("prefix", "A prefix to prepend to the tile set name")
      val xCol = argParser.getString("x", """The column to use for the X value of each node(defaults to "x")""", Some("x"))
      val yCol = argParser.getString("y", """The column to use for the Y value of each node(defaults to "y")""", Some("y"))
      val zkq = argParser.getString("zkq", "zookeeper quorum", Some("uscc0-master0.uncharted.software"))
      val zkp = argParser.getString("zkp", "zookeeper port", Some("2181"))
      val zkm = argParser.getString("xkm", "zookeeper master", Some("uscc0-master0.uncharted.software:60000"))
      val epsilon = 1E-16

      HeatmapCountValueExtractorFactory.register
      HeatmapFieldValueExtractorFactory.register

      // List of ((min tiling level, max tiling level), graph hierarchy level)
      val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds(0), bounds(1) - 1)).toList.reverse.zipWithIndex.reverse

      println("Overall start time: "+new Date())
      clusterAndGraphLevels.foreach { case ((minT, maxT), g) =>
        val tilingParameters = new TilingTaskParameters(
          tileSet, tileDesc.getOrElse(""), prefix,
          Seq((minT to maxT).toSeq), 256, 256, None, None)
        val hbaseParameters = Some(HBaseParameters(zkq, zkp, zkm))

        println
        println
        println
        println("Tiling hierarchy level " + g)
        println("\tmin tile level: " + minT)
        println("\tmax tile level: " + maxT)
        println("\tstart time: " + new Date())

        val loadStage: PipelineStage = PipelineStage("load level " + g, loadRawDataOp(base + "level_" + g)(_))
        val filterStage: Option[PipelineStage] = nodeTest.map { test =>
          PipelineStage("Filter raw data for nodes", regexFilterOp(test, DEFAULT_LINE_COLUMN)(_))
        }
        val CSVStage = PipelineStage("Convert to CSV", rawToCSVOp(getKVFile(nodeFileDescriptor))(_))
        val tilingStage = PipelineStage("Tiling level " + g,
          graphHeatMapOp(
            xCol, yCol, tilingParameters, hbaseParameters,
            bounds = Some(Bounds(0.0, 0.0, 256 - epsilon, 256 - epsilon))
          )
        )

        loadStage
          .addChild(filterStage)
          .addChild(CSVStage)
          .addChild(tilingStage)

        PipelineTree.execute(loadStage, sqlc)
        println("\tend time: " + new Date())
      }
      println("Overall end time: "+new Date())
      sc.stop
    } catch {
      case e: Exception => {
        e.printStackTrace()
        argParser.usage
      }
    }
  }
}
