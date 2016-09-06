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
package software.uncharted.graphing.salt


import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.graphing.config.GraphConfig
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.BasicSaltOperations
import software.uncharted.xdata.ops.util.BasicOperations
import software.uncharted.xdata.ops.util.DebugOperations
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TilingConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.OutputOperation


object NodeTilingPipeline extends Logging {
  def main(args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    // load properties file from supplied URI
    val config = GraphConfig.getFullConfiguration(args, this.logger)

    execute(config)
  }

  def execute (config: Config): Unit = {
    val sqlc = SparkConfig(config)
    try {
      execute(sqlc, config)
    } finally {
      sqlc.sparkContext.stop()
    }
  }

  def execute (sqlc: SQLContext, config: Config): Unit = {
    val tilingConfig = TilingConfig(config).getOrElse(errorOut("No tiling configuration given."))
    val outputConfig = JobUtil.createTileOutputOperation(config).getOrElse(errorOut("No output configuration given."))
    val graphConfig = GraphConfig(config).getOrElse(errorOut("No graph configuration given."))

    // calculate and save our tiles
    graphConfig.graphLevelsByHierarchyLevel.foreach { case ((minT, maxT), g) =>
      println(s"Tiling hierarchy level $g at tile levels $minT to $maxT")
      tileHierarchyLevel(sqlc, g, minT to maxT, tilingConfig, graphConfig, outputConfig)
    }
  }

  def getSchema(analytics: Seq[CustomGraphAnalytic[_]]): StructType = {
    // This schema must match that written by HierarchicalFDLayout.saveLayoutResult (the resultNodes variable)
    // "node\t" + id + "\t" + x + "\t" + y + "\t" + radius + "\t" + parentID + "\t" + parentX + "\t" + parentY + "\t" + parentR + "\t" + numInternalNodes + "\t" + degree + "\t" + metaData
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
        StructField("metadata", StringType)
      ) ++ analytics.map(a => StructField(a.getColumnName, StringType))
    )
  }

  def tileHierarchyLevel (sqlc: SQLContext,
                          hierarchyLevel: Int,
                          zoomLevels: Seq[Int],
                          tileConfig: TilingConfig,
                          graphConfig: GraphConfig,
                          outputOperation: OutputOperation): Unit = {
    import BasicOperations._
    import DebugOperations._
    import BasicSaltOperations._
    import software.uncharted.xdata.ops.{numeric => XDataNum}
    import software.uncharted.xdata.ops.{io => XDataIO}
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import RDDIO.mutateContextFcn

    val schema = getSchema(graphConfig.analytics)

    val tiles = Pipe(sqlc)
      .to(RDDIO.read(tileConfig.source + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Level $hierarchyLevel raw data: "))
      .to(regexFilter("^node.*"))
      .to(countRDDRowsOp("Node data: "))
      .to(toDataFrame(sqlc, Map[String, String]("delimiter" -> "\t", "quote" -> null), Some(schema)))
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
