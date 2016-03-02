package software.uncharted.graphing.salt


import software.uncharted.graphing.geometry.{Line, LineToPoints}

import scala.collection.mutable.{Buffer => MutableBuffer}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.{NumericProjection}
import software.uncharted.salt.core.spreading.SpreadingFunction
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe._

import scala.util.Try


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

/**
  * A cartesian projection of lines
  *
  * Input coordinates are the two endpoints of the line in the form (x1, y1, x2, y2)
  *
  * Tile coordinates are as normal
  *
  * Bin coordinates are the endpoints of the line, in <em>universal</em> bin coordinates
  *
  * @param zoomLevels
  * @param min the minimum value of a data-space coordinate
  * @param max the maximum value of a data-space coordinate
  * @param leaderLineLength The number of bins on each side of a line to keep
  */
class CartesianLeaderLineProjection(zoomLevels: Seq[Int],
                                    min: (Double, Double),
                                    max: (Double, Double),
                                    leaderLineLength: Int,
                                    tms: Boolean
                                   )
  extends NumericProjection[(Double, Double, Double, Double), (Int, Int, Int), ((Int, Int), (Int, Int))]((min._1, min._2, min._1, min._2), (max._1, max._2, max._1, max._2))
    with SegmentProjection {
  assert(max._1 > min._1)
  assert(max._2 > min._2)

  private val leaderLineLengthSquared = leaderLineLength * leaderLineLength
  // The X and Y range of our bounds; simple calculation, but we'll use it a lot.
  private val range = (max._1 - min._1, max._2 - min._2)
  // Translate an input coordinate into [0, 1) x [0, 1)
  private def translateAndScale (x: Double, y: Double): (Double, Double) =
    ((x - min._1) / range._1, (y - min._2) / range._2)
  /** Translate from a point in the range [0, 1) x [0, 1) into universal bin coordinates at the given level */
  private def scaledToUniversalBin (scaledPoint: (Double, Double), level: Int, maxBin: (Int, Int)): (Int, Int) = {
    val levelScale = 1L << level
    val levelX = scaledPoint._1 * levelScale
    val levelY = scaledPoint._2 * levelScale
    val tileX = levelX.floor.toInt
    val tileY = levelY.floor.toInt
    val binX = ((levelX - tileX) * (maxBin._1 + 1)).floor.toInt
    val binY = ((levelY - tileY) * (maxBin._2 + 1)).floor.toInt
    tileBinIndexToUniversalBinIndex((level, tileX, tileY), (binX, binY), maxBin, tms)
  }
  // Given an input universal coordinate and a number of bins-per-tile, find the top bin in the tile containing
  // said input
  private def getNextTileBoundary (input: Int, maxBin: Int, direction: Int): Int =
    if (direction > 0) input + (maxBin - (input % (maxBin + 1)))
    else input - (input % (maxBin + 1))

  private def boundaryPointsToEnd (line: LineToPoints, end: (Int, Int), maxBin: Int): Seq[(Int, Int)] = {
    val points = MutableBuffer[(Int, Int)]()
    val cur = line.current
    points += cur
    val direction = if (line.longAxisValue(cur) < line.longAxisValue(end)) 1 else -1

    var lap = line.curLongAxisPosition
    val lastBin = line.longAxisValue(end)
    var nextBoundary = getNextTileBoundary(lap, maxBin, direction)
    if (nextBoundary * direction > lastBin * direction) {
      points += end
    } else {
      var lastBoundary = getNextTileBoundary(lastBin, maxBin, direction) - maxBin * direction
      while (nextBoundary * direction < lastBoundary * direction) {
        val nb = line.skipTo(nextBoundary)
        if (nb != cur)
          points += nb
        nextBoundary += direction
        if (nextBoundary * direction < lastBin * direction)
          points += line.next()
        nextBoundary += maxBin * direction
      }
      points += end
    }
    points
  }


  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    * Here we just give tiles; to convert into individual bins, use the CartesianLeaderLineSpreadingFunction
    *
    * @param coordinates the data-space coordinates of the segment: start x, start y, end x, end y
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return Optional sequence representing a series of tile coordinate/bin index pairs if the given source
    *         row is within the bounds of the viz. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)],
                       maxBin: ((Int, Int), (Int, Int))): Option[Seq[((Int, Int, Int), ((Int, Int), (Int, Int)))]] = {
    if (!coordinates.isDefined) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)
      val realMaxBin = maxBin._1

      Some(zoomLevels.flatMap { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, realMaxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, realMaxBin)

        // These are recorded as the endpoints for every tile
        val endpoints = (startUBin, endUBin)

        // Now, we just need to find tiles
        // We go through the line, jumping to every point that is a segment endpoint or a boundary bin on a tile

        // We do this differently depending on whether or not we have to cut out middle points
        val line2point = new LineToPoints(startUBin, endUBin)
        val maxLABins = line2point.longAxisValue(realMaxBin)
        val lastLABin = line2point.lastLongAxisPosition
        val boundaryBins: Seq[(Int, Int)] =
          if (line2point.totalLength < 2 * leaderLineLength) {
            boundaryPointsToEnd(line2point, endUBin, maxLABins)
          } else {
            // Find first half points
            val startLeaderEnd = {
              var skipPoint = line2point.longAxisValue(line2point.skipToDistance(startUBin, leaderLineLength))
              line2point.reset
              var p =
                if (line2point.increasing) line2point.skipTo(skipPoint - 1)
                else line2point.skipTo(skipPoint + 1)
              var pn = p
              while (Line.distanceSquared(pn, startUBin) <= leaderLineLengthSquared) {
                p = pn
                pn = line2point.next()
              }
              p
            }
            val startLeaderEndLA = line2point.longAxisValue(startLeaderEnd)
            line2point.reset()
            val firstHalf = boundaryPointsToEnd(line2point, startLeaderEnd, maxLABins)
            line2point.reset()

            // Find the second half points
            val endLeaderStart = {
              var p = line2point.skipToDistance(endUBin, leaderLineLength+1)
              while (Line.distanceSquared(p, endUBin) > leaderLineLengthSquared)
                p = line2point.next()
              p
            }
            val endLeaderStartLA = line2point.longAxisValue(endLeaderStart)
            line2point.reset()
            line2point.skipTo(endLeaderStartLA + (if (line2point.increasing) -1 else 1))

            val secondHalf = boundaryPointsToEnd(line2point, endUBin, maxLABins)

            firstHalf ++ secondHalf
          }

        // We go to the boundary points between bins along the long axis of the line, picking the tile of each
        import Line._

        val bba = boundaryBins
          .filter { boundaryUBin =>
            distanceSquared(boundaryUBin, startUBin) <= leaderLineLengthSquared ||
              distanceSquared(boundaryUBin, endUBin) <= leaderLineLengthSquared
          }
        val bbb = bba.map(boundaryUBin => universalBinIndexToTileIndex(level, boundaryUBin, realMaxBin, tms)._1)
        val bbc = bbb.distinct
        val bbd = bbc.map(tile => (tile, endpoints))
        bbd
      })
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: ((Int, Int), (Int, Int)), maxBin: ((Int, Int), (Int, Int))): Int = {
    bin._1._1 + bin._1._2*(maxBin._1._1 + 1)
  }

}

class CartesianLeaderLineSpreadingFunction[T] (maxBin: (Int, Int), tms: Boolean)
  extends SpreadingFunction[(Int, Int, Int), (Int, Int, Int, Int), T]
    with SegmentProjection {
  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coords the visualization-space coordinates
    * @param value  the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  override def spread(coords: Seq[((Int, Int, Int), (Int, Int, Int, Int))], value: Option[T]): Seq[((Int, Int, Int), (Int, Int, Int, Int), Option[T])] = {
    coords.map { case (tileCoordinates, endPoints) =>
      val (level, tx, ty) = tileCoordinates
      val (uxStart, uyStart, uxEnd, uyEnd) = endPoints
      val uBounds = universalBinTileBounds(tileCoordinates, maxBin, tms)
      val line = new LineToPoints((uxStart, uyStart), (uxEnd, uyEnd))
        null
    }
    null
  }
}