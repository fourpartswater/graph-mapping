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
import java.util.Properties

import com.oculusinfo.tilegen.datasets.TilingTaskParameters
import com.oculusinfo.tilegen.pipeline.PipelineOperations._
import com.oculusinfo.tilegen.pipeline._
import com.oculusinfo.tilegen.util.{PropertiesWrapper, KeyValueArgumentSource, ArgumentParser}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object NodeTilingPipelineApp {
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


      // List of ((min tiling level, max tiling level), graph hierarchy level)
      val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds(0), bounds(1) - 1)).zipWithIndex

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
        val loadStage = PipelineStage("load level  " + g, loadCsvDataOp(base + "/level_" + g, getKVFile(nodeFileDescriptor))(_))
        val tilingStage = PipelineStage("Tiling level " + g,
          crossplotHeatMapOp(
            xCol, yCol, tilingParameters, hbaseParameters,
            bounds = Some(Bounds(0.0, 0.0, 256 - epsilon, 256 - epsilon))
          )
        )

        loadStage.addChild(tilingStage)

        PipelineTree.execute(loadStage, sqlc)
      }
      sc.stop
    } catch {
      case e: Exception => {
        e.printStackTrace()
        argParser.usage
      }
    }
  }
}
