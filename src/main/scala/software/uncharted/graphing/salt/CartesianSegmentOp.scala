package software.uncharted.graphing.salt



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.NumericProjection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe._



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
   valueColumns: Option[Seq[String]],
   xyBounds: (Double, Double, Double, Double),
   zBounds: (Int, Int),
   valueExtractor: Row => Option[T],
   binAggregator: Aggregator[T, U, V],
   tileAggregator: Option[Aggregator[V, W, X]],
   tileSize: Int
  )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), V, X]] = {
    // We need both coordinate and value columns
    val coordCols = Seq(x1Col, y2Col, x2Col, y2Col)
    val selectCols = valueColumns.map(coordCols union _).getOrElse(coordCols).distinct.map(new Column(_))

    val data = Pipe(input)
      .to(castColumns(Map(x1Col -> "double", y1Col -> "double", x2Col -> "double", y2Col -> "double")))
      .to(_.select(selectCols:_*))
      .run

    val coordinateExtractor = (r: Row) =>
      if (!r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2) && !r.isNullAt(3)) {
        Some((r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getDouble(3)))
      } else {
        None
      }

//    val series = new Series((
//      // Maximum bin indices
//      (tileSize - 1, tileSize - 1),
//      coordinateExtractor,
//      projection,
//      valueExtractor,
//      binAggregator,
//      tileAggregator,
//      None)

//    val foo: CartesianProjection
    null
  }
}

