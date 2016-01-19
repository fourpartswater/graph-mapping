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

import com.oculusinfo.tilegen.datasets.{LineDrawingType, TilingTaskParameters}
import com.oculusinfo.tilegen.pipeline.{Bounds, PipelineStage, HBaseParameters}
import com.oculusinfo.tilegen.util.{ArgumentParser, PropertiesWrapper, KeyValueArgumentSource}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object EdgeTilingPipelineApp {
  import com.oculusinfo.tilegen.pipeline.PipelineOperations._
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
      val edgeFileDescriptor = argParser.getString("edgeColumns", "A file containing parse information for the edge lines")
      val tileSet = argParser.getString("name", "The name of the edge tile set to produce")
      val tileDesc = argParser.getStringOption("desc", "A description of the edge tile set to produce")
      val prefix = argParser.getStringOption("prefix", "A prefix to prepend to the tile set name")
      val x1Col = argParser.getString("x1", """The column to use for the X value of the source of each edge (defaults to "x1")""", Some("x1"))
      val y1Col = argParser.getString("y1", """The column to use for the Y value of the source of each edge (defaults to "y1")""", Some("y1"))
      val x2Col = argParser.getString("x2", """The column to use for the X value of the destination of each edge (defaults to "x2")""", Some("x2"))
      val y2Col = argParser.getString("y2", """The column to use for the Y value of the destination of each edge (defaults to "y2")""", Some("y2"))
      val lineType = argParser.getStringOption("lineType", """The type of line to draw (leader, arc, or line)""").map(_.toLowerCase) match {
        case Some("leader") => Some(LineDrawingType.LeaderLines)
        case Some("arc") => Some(LineDrawingType.Arcs)
        case Some("line") => Some(LineDrawingType.Lines)
        case None => None
        case Some(other) => throw new Exception("Illegal value for line type: "+other)
      }
      val minSegLen = argParser.getIntOption("minLength", """The minimum segment length to draw (used when lineType="line" or "arc")""")
      val maxSegLen = argParser.getIntOption("maxLength", """When lineType="line" or "arc", the maximum segment length to draw.  When lineType="leader", the maximum leader length to draw""")
      val zkq = argParser.getString("zkq", "zookeeper quorum", Some("uscc0-master0.uncharted.software"))
      val zkp = argParser.getString("zkp", "zookeeper port", Some("2181"))
      val zkm = argParser.getString("xkm", "zookeeper master", Some("uscc0-master0.uncharted.software:60000"))
      val epsilon = 1E-16


      // List of ((min tiling level, max tiling level), graph hierarchy level)
      val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds(0), bounds(1) - 1)).toList.reverse.zipWithIndex.reverse
      val bounds = Bounds(0.0, 0.0, 256.0 - epsilon, 256.0 - epsilon)
      val hbaseParameters = Some(HBaseParameters(zkq, zkp, zkm))

      clusterAndGraphLevels.foreach { case ((minT, maxT), g) =>
        val tilingParameters = new TilingTaskParameters(
          tileSet, tileDesc.getOrElse(""), prefix,
          Seq((minT to maxT).toSeq), 256, 256, None, None)

        val loadStage = PipelineStage("load level  " + g, loadCsvDataOp(base + "/level_" + g, getKVFile(edgeFileDescriptor))(_))
        val tilingStage = PipelineStage("Tiling level " + g,
          segmentTilingOp(x1Col, y1Col, x2Col, y2Col, Some(bounds), tilingParameters, hbaseParameters, lineType, minSegLen, maxSegLen, maxSegLen)(_)
        )
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
