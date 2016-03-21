package software.uncharted.graphing.salt


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.mapreduce.MapReduceTileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.NumericProjection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe._

import scala.util.Try


object CartesianSegmentOp {
  def apply[T, U, V, W, X]
  (arcType: ArcTypes.Value,
   x1Col: String,
   y1Col: String,
   x2Col: String,
   y2Col: String,
   xyBounds: (Double, Double, Double, Double),
   zBounds: (Int, Int),
   valueExtractor: Option[Row => Option[T]],
   binAggregator: Aggregator[T, U, V],
   tileAggregator: Option[Aggregator[V, W, X]],
   tileSize: Int
  )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), V, X]] = {
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
      case ArcTypes.FullLine => new SimpleLineProjection(zoomLevels, minBounds, maxBounds, tms = true)
      case ArcTypes.LeaderLine => new SimpleLeaderLineProjection(zoomLevels, minBounds, maxBounds, leaderLineLength, tms = true)
      case ArcTypes.FullArc => new SimpleArcProjection(zoomLevels, minBounds, maxBounds, tms = true)
      case ArcTypes.LeaderArc => new SimpleLeaderArcProjection(zoomLevels, minBounds, maxBounds, leaderLineLength, tms = true)
    }

    // Put together a series object to encapsulate our tiling job
    val series = new Series(
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
    val generator = new MapReduceTileGenerator(sc)

    generator.generate(input.rdd, series, request).map(t => series(t))
  }
}

object ArcTypes extends Enumeration {
  val FullLine, LeaderLine, FullArc, LeaderArc = Value
}
