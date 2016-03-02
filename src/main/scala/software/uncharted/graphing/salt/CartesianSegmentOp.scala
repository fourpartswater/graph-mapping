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

trait SegmentProjection {
  /**
    * Change from a (tile, bin) coordinate to a (universal bin) coordinate
    *
    * Generally, the upper left corner is taken as (0, 0).  If TMS is specified, then the tile Y coordinate is
    * flipped (i.e., lower left is (0, 0)), but the bin coordinates (both tile-bin and universal-bin) are not.
    *
    * @param tile the tile coordinate
    * @param bin the bin coordinate
    * @param maxBin the maximum bin index within each tile
    * @param tms if true, the Y axis for tile coordinates only is flipped
    * @return The universal bin coordinate of the target cell, with (0, 0) being the upper left corner of the whole
    *         space
    */
  protected def tileBinIndexToUniversalBinIndex(tile: (Int, Int, Int),
                                                bin: (Int, Int),
                                                maxBin: (Int, Int),
                                                tms: Boolean): (Int, Int) = {
    val pow2 = 1 << tile._1

    val tileLeft = tile._2 * (maxBin._1+1)

    val tileTop = tms match {
      case true => (pow2 - tile._3 - 1)*(maxBin._2+1)
      case false => tile._3*(maxBin._2+1)
    }

    (tileLeft + bin._1, tileTop + bin._2)
  }

  /**
    * Change from a (universal bin) coordinate to a (tile, bin) coordinate.
    *
    * Generally, the upper left corner is taken as (0, 0).  If TMS is specified, then the tile Y coordinate is
    * flipped (i.e., lower left is (0, 0)), but the bin coordinates (both tile-bin and universal-bin) are not.
    *
    * @param z The zoom level of the point in question
    * @param universalBin The universal bin coordinate of the input point
    * @param maxBin the maximum bin index within each tile
    * @param tms if true, the Y axis for tile coordinates only is flipped
    * @return The tile and bin at the given level of the given universal bin
    */
  protected def universalBinIndexToTileIndex(z: Int,
                                             universalBin: (Int, Int),
                                             maxBin: (Int, Int),
                                             tms: Boolean) = {
    val pow2 = 1 << z

    val xBins = (maxBin._1+1)
    val yBins = (maxBin._2+1)

    val tileX = universalBin._1/xBins
    val binX = universalBin._1 - tileX * xBins;

    val tileY = tms match {
      case true => pow2 - (universalBin._2/yBins) - 1;
      case false => universalBin._2/yBins
    }

    val binY = tms match {
      case true => universalBin._2 - ((pow2 - tileY - 1) * yBins)
      case false => universalBin._2 - (tileY) * yBins
    }

    ((z, tileX, tileY), (binX, binY))
  }

  def universalBinTileBounds (tile: (Int, Int, Int), maxBin: (Int,Int), tms: Boolean): ((Int, Int),(Int, Int)) = {
    val ul = tileBinIndexToUniversalBinIndex(tile, (0, 0), maxBin, tms)
    val lr = tileBinIndexToUniversalBinIndex(tile, maxBin, maxBin, tms)
    (ul, lr)
  }
}

