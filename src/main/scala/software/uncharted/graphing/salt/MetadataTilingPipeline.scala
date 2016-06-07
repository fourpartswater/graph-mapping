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



import org.apache.spark.rdd.RDD
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.projection.numeric.CartesianProjection
import software.uncharted.salt.core.util.SparseArray

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import software.uncharted.graphing.utilities.ArgumentParser
import software.uncharted.sparkpipe.Pipe



object MetadataTilingPipeline {
  def main(args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    val argParser = new ArgumentParser(args)

    val sc = new SparkContext(new SparkConf().setAppName("MetaData Tiling Pipeline"))
    val sqlc = new SQLContext(sc)

    val base = argParser.getStringOption("base", "The base location of graph layout information", None).get
    val levels = argParser.getIntSeq("levels",
      """The number of levels per hierarchy level.  These will be applied in reverse order -
        | so that a value of "4,3,2" means that hierarchy level 2 will be used for tiling
        | levels 0-3, hierarcy level 1 will be used for tiling levels 4-6, and hierarchy
        | level 0 will be used for tiling levels 7 and 8.""".stripMargin, None)

    val tableName = argParser.getStringOption("name", "The name of the node tile set to produce", None).get
    val familyName = argParser.getString("column", "The column into which to write tiles", "tileData")
    val qualifierName = argParser.getString("column-qualifier", "A qualifier to use on the tile column when writing tiles", "")
    val customAnalytics = argParser.getStrings("analytic", "Node analytics that are recorded on each node, needed to parse node lines")
      .map(CustomGraphAnalytic.apply)

    // Get the tiling levels corresponding to each hierarchy level
    val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds.head, bounds.last - 1)).toList.reverse.zipWithIndex.reverse

    // calculate and save our tiles
    clusterAndGraphLevels.foreach { case ((minT, maxT), g) =>
      tileHierarchyLevel(sqlc)(base, g, minT to maxT, tableName, familyName, qualifierName, /* hbaseConfiguration */ null, customAnalytics)
    }

    sc.stop()
  }

  def tileHierarchyLevel(sqlc: SQLContext)(
    path: String,
    hierarchyLevel: Int,
    zoomLevels: Seq[Int],
    tableName: String,
    familyName: String,
    qualifierName: String,
    hbaseConfiguration: Configuration,
    analytics: Seq[CustomGraphAnalytic[_]]): Unit = {
    import GraphTilingOperations._
    import DebugGraphOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import software.uncharted.xdata.ops.{io => XDataIO}
    import RDDIO.mutateContextFcn

    val rawData = Pipe(sqlc)
      .to(RDDIO.read(path + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Input data for hierarchy level $hierarchyLevel: "))

    //                      RT = row type                       DC = data c.      TC - tile c.     BC = Bin c.
    val series = new Series[((Double, Double), GraphCommunity), (Double, Double), (Int, Int, Int), (Int, Int),
      // T = data type  U, V = binTypes
      GraphCommunity, GraphRecord, GraphRecord, GraphRecord, GraphRecord](
      (0, 0),
      r => {
        Some(r._1)
      },
      new CartesianProjection(zoomLevels, (0.0, 0.0), (256.0, 256.0)),
      r => {
        Some(r._2)
      },
      new GraphMetadataAggregator
    )

    // Get our VertexRDD
    val nodeSchema = NodeTilingPipeline.getSchema(analytics)
    val nodeData = rawData
      .to(regexFilter("^node.*"))
      .to(countRDDRowsOp("Node data: "))
      .to(toDataFrame(sqlc, Map[String, String]("delimiter" -> "\t", "quote" -> null), Some(nodeSchema)))
      .to(countDFRowsOp("Parsed node data: "))
      .to(parseNodes(hierarchyLevel, analytics))


    //      .to(countRDDRowsOp("Graph nodes: "))

    // Get our EdgeRDD
    val edgeSchema = EdgeTilingPipeline.getSchema
    val edgeData = rawData
      .to(regexFilter("^edge.*"))
      .to(countRDDRowsOp("Edge data: "))
      .to(toDataFrame(sqlc, Map[String, String]("delimiter" -> "\t", "quote" -> null), Some(edgeSchema)))
      .to(countDFRowsOp("Parsed edge data: "))
      .to(parseEdges(hierarchyLevel, weighted = true, specifiesExternal = true))
      .to(countRDDRowsOp("Graph edges: "))

    // Combine them into communities
    val communityData = Pipe(nodeData, edgeData)
      .to(consolidateCommunities(analytics))
      .to(countRDDRowsOp("Communities: "))

    // Tile the communities
    val getZoomLevel: ((Int, Int, Int)) => Int = _._1

    val encodeTile: SparseArray[GraphRecord] => Seq[Byte] = tileData => {
      // The input array should really only have one bin
      tileData.length() match {
        case 0 => Seq[Byte]()
        case 1 => tileData.apply(0).toString(10).getBytes
        case _ => throw new Exception("Expected tiles with a single record only, got a tile with " +
          tileData.length() + " records")
      }
    }

    val awsAccessKey = System.getenv("AWS_ACCESS_KEY")
    val awsSecretKey = System.getenv("AWS_SECRET_KEY")

    communityData
      .to(genericFullTilingRequest(series, zoomLevels, getZoomLevel))
      .to(countRDDRowsOp("Tiles: "))
      .to(serializeTiles(encodeTile))
      .to(XDataIO.writeToS3(awsAccessKey, awsSecretKey, "", tableName))
      .run()
  }

  def parseNodes(hierarchyLevel: Int, analytics: Seq[CustomGraphAnalytic[_]])(rawData: DataFrame): RDD[(Long, GraphCommunity)] = {
    def getDefaultAnalyticValue[T](analytic: CustomGraphAnalytic[T]): String =
      analytic.aggregator.finish(analytic.aggregator.default())

    // Names must match those in NodeTilingPipeline schema
    val idExtractor = new LongExtractor(rawData, "nodeId", None)
    val xExtractor = new DoubleExtractor(rawData, "x", None)
    val yExtractor = new DoubleExtractor(rawData, "y", None)
    val rExtractor = new DoubleExtractor(rawData, "r", Some(0.0))
    val degreeExtractor = new IntExtractor(rawData, "degree", Some(0))
    val numNodesExtractor = new LongExtractor(rawData, "internalNodes", Some(0))
    val metadataExtractor = new StringExtractor(rawData, "metadata", Some(""))
    val parentIdExtractor = new LongExtractor(rawData, "parentId", Some(-1L))
    val parentXExtractor = new DoubleExtractor(rawData, "parentX", Some(-1.0))
    val parentYExtractor = new DoubleExtractor(rawData, "parentY", Some(-1.0))
    val parentRExtractor = new DoubleExtractor(rawData, "parentR", Some(0.0))
    val analyticExtractors = analytics.map { a =>
      new StringExtractor(rawData, a.getColumnName, Some(getDefaultAnalyticValue(a)))
    }.toArray.toSeq
    rawData.rdd.flatMap { row =>
      Try {
        val id = idExtractor.getValue(row)
        val x = xExtractor.getValue(row)
        val y = yExtractor.getValue(row)
        val r = rExtractor.getValue(row)
        val degree = degreeExtractor.getValue(row)
        val numNodes = numNodesExtractor.getValue(row)
        val metadata = metadataExtractor.getValue(row)
        val pId = parentIdExtractor.getValue(row)
        val px = parentXExtractor.getValue(row)
        val py = parentYExtractor.getValue(row)
        val pr = parentRExtractor.getValue(row)
        val analyticValues = analyticExtractors.map { extractor =>
          extractor.getValue(row)
        }

        (id, new GraphCommunity(hierarchyLevel, id, (x, y), r, degree, numNodes, metadata, id == pId, pId, (px, py), pr, analyticValues))
      }.toOption
    }
  }

  def parseEdges[T](hierarchyLevel: Int, weighted: Boolean, specifiesExternal: Boolean)
                   (rawData: DataFrame): RDD[(Long, (GraphEdge, Boolean))] = {
    // Names must match those in EdgeTilingPipeline schema
    val srcIdExtractor = new LongExtractor(rawData, "srcId", None)
    val srcXExtractor = new DoubleExtractor(rawData, "srcX", None)
    val srcYExtractor = new DoubleExtractor(rawData, "srcY", None)
    val dstIdExtractor = new LongExtractor(rawData, "dstId", None)
    val dstXExtractor = new DoubleExtractor(rawData, "dstX", None)
    val dstYExtractor = new DoubleExtractor(rawData, "dstY", None)
    val weightExtractor =
      if (weighted) Some(new LongExtractor(rawData, "weight", None))
      else None
    val externalExtractor =
      if (specifiesExternal) Some(new IntExtractor(rawData, "isInterCommunity", None))
      else None

    rawData.rdd.flatMap { row =>
      Try {
        val srcId = srcIdExtractor.getValue(row)
        val srcX = srcXExtractor.getValue(row)
        val srcY = srcYExtractor.getValue(row)
        val dstId = dstIdExtractor.getValue(row)
        val dstX = dstXExtractor.getValue(row)
        val dstY = dstYExtractor.getValue(row)
        val weight = weightExtractor.map(_.getValue(row)).getOrElse(1L)
        val external = externalExtractor.map(_.getValue(row)).getOrElse(1) == 1

        if (-1 == srcId || -1 == dstId || srcId == dstId)
          throw new Exception("Irrelevant edge")

        Iterator(
          (srcId, (new GraphEdge(dstId, (dstX, dstY), weight), external)),
          (dstId, (new GraphEdge(srcId, (srcX, srcY), weight), external))
        )
      }.toOption.getOrElse(Iterator[(Long, (GraphEdge, Boolean))]())
    }
  }

  def consolidateCommunities(analytics: Seq[CustomGraphAnalytic[_]])
                            (input: (RDD[(Long, GraphCommunity)], RDD[(Long, (GraphEdge, Boolean))])):
  RDD[((Double, Double), GraphCommunity)] = {
    val (vertices, edges) = input
    type EdgeListOption = Option[MutableBuffer[GraphEdge]]
    val getCombineEdgeList: (EdgeListOption, EdgeListOption) => EdgeListOption = (a, b) => {
      if (a.isDefined) {
        b.foreach(_.foreach(edge => GraphCommunity.addEdgeInPlace(a.get, edge)))
        a
      } else {
        b
      }
    }

    val edgeLists = edges.map { case (id, (edge, external)) =>
      if (external) {
        (id, (Some(MutableBuffer(edge)), None: Option[MutableBuffer[GraphEdge]]))
      } else {
        (id, (None: Option[MutableBuffer[GraphEdge]], Some(MutableBuffer(edge))))
      }
    }.reduceByKey((a, b) =>
      (getCombineEdgeList(a._1, b._1), getCombineEdgeList(a._2, b._2))
    )

    vertices.leftOuterJoin(edgeLists).map { case (id, (community, edgesOption)) =>
      val coordinates = community.coordinates
      val newCommunity = new GraphCommunity(
        community.hierarchyLevel,
        community.id,
        community.coordinates,
        community.radius,
        community.degree,
        community.numNodes,
        community.metadata,
        community.isPrimaryNode,
        community.parentId,
        community.parentCoordinates,
        community.parentRadius,
        community.analyticValues,
        edgesOption.map(_._1).getOrElse(None),
        edgesOption.map(_._2).getOrElse(None)
      )
      (coordinates, newCommunity)
    }
  }
}

abstract class ValueExtractor[T](data: DataFrame, columnName: String, defaultValue: Option[T]) extends Serializable {
  val columnIndex = data.schema.fieldIndex(columnName)

  def getValue(row: Row): T = {
    // Throws an error if no value in this row and no default value
    Try(getValueInternal(row, columnIndex)).getOrElse(defaultValue.get)
  }

  protected def getValueInternal(row: Row, index: Int): T
}

class IntExtractor(data: DataFrame, columnName: String, defaultValue: Option[Int])
  extends ValueExtractor[Int](data, columnName, defaultValue) {
  override protected def getValueInternal(row: Row, index: Int): Int = row.getInt(index)
}

class LongExtractor(data: DataFrame, columnName: String, defaultValue: Option[Long])
  extends ValueExtractor[Long](data, columnName, defaultValue) {
  override protected def getValueInternal(row: Row, index: Int): Long = row.getLong(index)
}

class DoubleExtractor(data: DataFrame, columnName: String, defaultValue: Option[Double])
  extends ValueExtractor[Double](data, columnName, defaultValue) {
  override protected def getValueInternal(row: Row, index: Int): Double = row.getDouble(index)
}

class StringExtractor(data: DataFrame, columnName: String, defaultValue: Option[String])
  extends ValueExtractor[String](data, columnName, defaultValue) {
  override protected def getValueInternal(row: Row, index: Int): String = row.getString(index)
}
