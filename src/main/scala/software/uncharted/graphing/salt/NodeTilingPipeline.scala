/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.salt


import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._ //scalastyle:ignore
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.graphing.config.GraphConfig
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.BasicSaltOperations
import software.uncharted.xdata.ops.util.{BasicOperations, DataFrameOperations, DebugOperations}
import software.uncharted.xdata.sparkpipe.config.{TilingConfig}
import software.uncharted.xdata.sparkpipe.jobs.{AbstractJob, JobUtil}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.OutputOperation

import scala.util.{Failure, Success}

//scalastyle:off null underscore.import import.grouping
/**
  * A job to produce a set of tiles showing the nodes in a hierarchically clustered graph.
  *
  * The input to this job should be the output of the ClusteredGraphLayoutApp.
  *
  * The output is a tile set.
  */
object NodeTilingPipeline extends AbstractJob {

  def execute (sparkSession: SparkSession, config: Config): Unit = {
    val tilingConfig = TilingConfig(config).getOrElse(errorOut("No tiling configuration given."))
    val outputConfig = JobUtil.createTileOutputOperation(config).getOrElse(errorOut("No output configuration given."))

    val graphConfig = GraphConfig.parse(config) match {
      case Success(s) => s
      case Failure(f) =>
        error("Couldn't read graph configuration", f)
        sys.exit(-1)
    }

    // calculate and save our tiles
    graphConfig.graphLevelsByHierarchyLevel.foreach { case ((minT, maxT), g) =>
      println(s"Tiling hierarchy level $g at tile levels $minT to $maxT")
      tileHierarchyLevel(sparkSession, g, minT to maxT, tilingConfig, graphConfig, outputConfig)
    }
  }

  def getSchema(analytics: Seq[CustomGraphAnalytic[_]]): StructType = {
    // This schema must match that written by HierarchicalFDLayout.saveLayoutResult (the resultNodes variable)
    // "node\t" + id + "\t" + x + "\t" + y + "\t" + radius + "\t" + parentID + "\t" + parentX + "\t" + parentY + "\t"
    // + parentR + "\t" + numInternalNodes + "\t" + degree + "\t" + metaData
    StructType(
      Seq(
        StructField("fieldType", StringType),
        StructField("nodeId", LongType),
        StructField("x", DoubleType),
        StructField("y", DoubleType),
        StructField("r", DoubleType),
        StructField("parentId", LongType),
        StructField("parentX", DoubleType),
        StructField("parentY", DoubleType),
        StructField("parentR", DoubleType),
        StructField("internalNodes", LongType),
        StructField("degree", IntegerType),
        StructField("level", IntegerType),
        StructField("metadata", StringType)
      ) ++ analytics.map(a => StructField(a.getColumnName, StringType))
    )
  }

  def tileHierarchyLevel (sparkSession: SparkSession,
                          hierarchyLevel: Int,
                          zoomLevels: Seq[Int],
                          tileConfig: TilingConfig,
                          graphConfig: GraphConfig,
                          outputOperation: OutputOperation): Unit = {
    import BasicOperations._
    import BasicSaltOperations._
    import DataFrameOperations._
    import DebugOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import software.uncharted.xdata.ops.{io => XDataIO, numeric => XDataNum}

    val schema = getSchema(graphConfig.analytics)

    val tiles = Pipe(sparkSession.sparkContext)
      .to(RDDIO.read(tileConfig.source + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Level $hierarchyLevel raw data: "))
      .to(regexFilter("^node.*"))
      .to(countRDDRowsOp("Node data: "))
      .to(toDataFrame(sparkSession, Map[String, String]("delimiter" -> "\t", "quote" -> null), schema))
      .to(countDFRowsOp("Parsed data: "))
      .to(XDataNum.addConstantColumn("count", 1))
      .to(cartesianTiling("x", "y", "count", zoomLevels, Some((0.0, 0.0, 256.0, 256.0))))
      .to(countRDDRowsOp("Tiles: "))
      .to(XDataIO.serializeBinArray)
      .to(outputOperation)
      .run()
  }

  private def errorOut (errorMsg: String) = {
    error(errorMsg)
    sys.exit(-1)
  }
}
//scalastyle:on null underscore.import import.grouping
