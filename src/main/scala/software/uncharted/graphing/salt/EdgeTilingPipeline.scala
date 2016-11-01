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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import software.uncharted.graphing.config.GraphConfig
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.BasicSaltOperations
import software.uncharted.xdata.ops.util.BasicOperations
import software.uncharted.xdata.ops.util.DebugOperations
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TilingConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.OutputOperation


object EdgeTilingPipeline extends Logging {

  def main(args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    // load properties file from supplied URI
    val config = GraphConfig.getFullConfiguration(args, this.logger)

    execute(config)
  }

  def execute (config: Config): Unit = {
    val sparkSession = SparkConfig(config)
    try {
      execute(sparkSession, config)
    } finally {
      sparkSession.sparkContext.stop()
    }
  }

  def execute (sparkSession: SparkSession, config: Config): Unit = {
    val tilingConfig = TilingConfig(config).getOrElse(errorOut("No tiling configuration given."))
    val outputConfig = JobUtil.createTileOutputOperation(config).getOrElse(errorOut("No output configuration given."))
    val graphConfig = GraphConfig(config).getOrElse(errorOut("No graph configuration given."))

    // calculate and save our tiles
    graphConfig.graphLevelsByHierarchyLevel.foreach { case ((minT, maxT), g) =>
      tileHierarchyLevel(sparkSession, g, minT to maxT, tilingConfig, graphConfig, outputConfig)
    }
  }

  def getSchema: StructType = {
    StructType(Seq(
      StructField("fieldType", StringType),
      StructField("srcId", LongType),
      StructField("srcX", DoubleType),
      StructField("srcY", DoubleType),
      StructField("dstId", LongType),
      StructField("dstX", DoubleType),
      StructField("dstY", DoubleType),
      StructField("weight", LongType),
      StructField("isInterCommunity", IntegerType)
    ))
  }

  def tileHierarchyLevel (sparkSession: SparkSession,
                          hierarchyLevel: Int,
                          zoomLevels: Seq[Int],
                          tileConfig: TilingConfig,
                          graphConfig: GraphConfig,
                          outputOperation: OutputOperation): Unit = {
    import BasicOperations._
    import DebugOperations._
    import BasicSaltOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import software.uncharted.xdata.ops.{io => XDataIO}
    import RDDIO.mutateContextFcn

    val edgeFcn: Option[DataFrame => DataFrame] = graphConfig.edgeType.map {value =>
      filterA(new Column("isInterCommunity") === value)
    }

    Pipe(sparkSession.sparkContext)
      .to(RDDIO.read(tileConfig.source + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Level $hierarchyLevel raw data: "))
      .to(regexFilter("^edge.*"))
      .to(countRDDRowsOp("Edge data: "))
      .to(toDataFrame(sparkSession, Map[String, String]("delimiter" -> "\t", "quote" -> null),
                      Some(getSchema)))
      .to(countDFRowsOp("Parsed data: "))
      .to(optional(edgeFcn))
      .to(countDFRowsOp("Required edges: " ))
      .to(segmentTiling("srcX", "srcY", "dstX", "dstY", zoomLevels, graphConfig.formatType, graphConfig.minSegLength, graphConfig.maxSegLength, Some((0.0, 0.0, 256.0, 256.0))))
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
