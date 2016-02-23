package software.uncharted.graphing.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.{CartesianProjection, NumericProjection}

object CartesianSegmentOp {
  def apply[T, U, V, W, X]
  (projection:
   NumericProjection[
     (Double, Double, Double, Double),
     (Int, Int, Int, Int, Int),
     (Int, Int, Int, Int)],
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
  )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), V, X]] = {
    val projection = new CartesianProjection(zBounds._1 to zBounds._2, (xyBounds._3, xyBounds._1), (xyBounds._4, xyBounds._2))
    val selectCols = Seq(x1Col, y2Col, x2Col, y2Col).union(input.schema.map(_.name).toSeq).distinct.map(new Column(_))
    null
  }
}
