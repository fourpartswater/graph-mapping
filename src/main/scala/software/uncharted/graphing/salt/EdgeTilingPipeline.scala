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
import org.apache.spark.sql.types._ //scalastyle:ignore
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import software.uncharted.graphing.config.GraphConfig
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.BasicSaltOperations
import software.uncharted.xdata.ops.util.BasicOperations
import software.uncharted.xdata.ops.util.DebugOperations
import software.uncharted.xdata.ops.util.DataFrameOperations
import software.uncharted.xdata.sparkpipe.config.TilingConfig
import software.uncharted.xdata.sparkpipe.jobs.AbstractJob
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.OutputOperation

import scala.util.{Failure, Success}


/**
  * A job to produce a set of tiles showing the edges in a hierarchically clustered graph.
  *
  * The input to this job should be the output of the ClusteredGraphLayoutApp.
  *
  * The output is a tile set.
  */
//scalastyle:off null underscore.import import.grouping
object EdgeTilingPipeline extends AbstractJob {
  def execute (session: SparkSession, config: Config): Unit = {
    val tilingConfig = parseTilingParameters(config)
    val outputConfig = parseOutputOperation(config)

    val graphConfig = GraphConfig.parse(config) match {
      case Success(s) => s
      case Failure(f) =>
        error("Couldn't read graph configuration", f)
        sys.exit(-1)
    }

    // calculate and save our tiles
    graphConfig.graphLevelsByHierarchyLevel.foreach { case ((minT, maxT), g) =>
      tileHierarchyLevel(session, g, minT to maxT, tilingConfig, graphConfig, outputConfig)
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

  def tileHierarchyLevel (session: SparkSession,
                          hierarchyLevel: Int,
                          zoomLevels: Seq[Int],
                          tileConfig: TilingConfig,
                          graphConfig: GraphConfig,
                          outputOperation: OutputOperation): Unit = {
    import DataFrameOperations._
    import BasicOperations._
    import BasicSaltOperations._
    import DataFrameOperations._
    import DebugOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import software.uncharted.xdata.ops.{io => XDataIO}

    val edgeFcn: Option[DataFrame => DataFrame] = graphConfig.edgeType.map {value =>
      filterA(new Column("isInterCommunity") === value)
    }

    Pipe(session.sparkContext)
      .to(RDDIO.read(tileConfig.source + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Level $hierarchyLevel raw data: "))
      .to(regexFilter("^edge.*"))
      .to(countRDDRowsOp("Edge data: "))
      .to(toDataFrame(session, Map[String, String]("delimiter" -> "\t", "quote" -> null), getSchema))
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
//scalastyle:on null underscore.import import.grouping
