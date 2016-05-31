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
import org.apache.spark.sql.{DataFrame, Row}

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.rdd.RDDTileGenerator
import software.uncharted.salt.core.generation.request.TileRequest

import scala.util.Try


object CartesianSegmentOp {
  def apply[T, U, V, W, X]
  (arcType: ArcTypes.Value,
   minSegLen: Option[Int],
   maxSegLen: Option[Int],
   x1Col: String,
   y1Col: String,
   x2Col: String,
   y2Col: String,
   xyBounds: (Double, Double, Double, Double),
   zBounds: (Int, Int),
   valueExtractor: Row => Option[T],
   binAggregator: Aggregator[T, U, V],
   tileAggregator: Option[Aggregator[V, W, X]],
   tileSize: Int
  )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {
    // A coordinate extractor to pull out our endpoint coordinates
    val x1Pos = input.schema.zipWithIndex.find(_._1.name == x1Col).map(_._2).getOrElse(-1)
    val y1Pos = input.schema.zipWithIndex.find(_._1.name == y1Col).map(_._2).getOrElse(-1)
    val x2Pos = input.schema.zipWithIndex.find(_._1.name == x2Col).map(_._2).getOrElse(-1)
    val y2Pos = input.schema.zipWithIndex.find(_._1.name == y2Col).map(_._2).getOrElse(-1)
    val coordinateExtractor = (row: Row) =>
        Try((row.getDouble(x1Pos), row.getDouble(y1Pos), row.getDouble(x2Pos), row.getDouble(y2Pos))).toOption

    // Figure out our projection
    val zoomLevels = zBounds._1 to zBounds._2
    val minBounds = (xyBounds._1, xyBounds._2)
    val maxBounds = (xyBounds._3, xyBounds._4)
    val leaderLineLength = 1024
    val projection = arcType match {
      case ArcTypes.FullLine => new SimpleLineProjection(zoomLevels, minBounds, maxBounds,
        minLengthOpt = minSegLen, maxLengthOpt = maxSegLen, tms = true)
      case ArcTypes.LeaderLine => new SimpleLeaderLineProjection(zoomLevels, minBounds, maxBounds, leaderLineLength,
        minLengthOpt = minSegLen, maxLengthOpt = maxSegLen, tms = true)
      case ArcTypes.FullArc => new SimpleArcProjection(zoomLevels, minBounds, maxBounds,
        minLengthOpt = minSegLen, maxLengthOpt = maxSegLen, tms = true)
      case ArcTypes.LeaderArc => new SimpleLeaderArcProjection(zoomLevels, minBounds, maxBounds, leaderLineLength,
        minLengthOpt = minSegLen, maxLengthOpt = maxSegLen, tms = true)
    }

    // Put together a series object to encapsulate our tiling job
    val series: Series[
      Row,                              // RT
      (Double, Double, Double, Double), // DC
      (Int, Int, Int),                  // TC
      (Int, Int),                       // BC
      T, U, V, W, X] = new Series(
      // Maximum bin indices
      (tileSize - 1, tileSize - 1),
      coordinateExtractor,
      projection,
      valueExtractor,
      binAggregator,
      tileAggregator,
      None
    )

    val sc = input.sqlContext.sparkContext
    val generator = new RDDTileGenerator(sc)

    generator.generate(input.rdd, series, request).flatMap(t => series(t))
  }
}

object ArcTypes extends Enumeration {
  val FullLine, LeaderLine, FullArc, LeaderArc = Value
}
