package software.uncharted.graphing.salt

import software.uncharted.salt.core.spreading.SpreadingFunction

import scala.collection.mutable.{Buffer => MutableBuffer}

import software.uncharted.graphing.geometry.{CartesianBinning, CartesianTileProjection2D, Line, LineToPoints}
import software.uncharted.salt.core.projection.numeric.NumericProjection



/**
  * This class projects segments into the tiles they intersect.  This is the first part of a two-stage process, the
  * second stage of which will take those tiles, and project the line further into individual bins.
  *
  * Input coordinates are the two endpoints of the line in the form (x1, y1, x2, y2)
  *
  * Tile coordinates are as normal
  *
  * Bin coordinates are the endpoints of the line, in <em>universal</em> bin coordinates
  *
  * @param zoomLevels the levels over which we are projecting lines
  * @param min the minimum value of a data-space coordinate
  * @param max the maximum value of a data-space coordinate
  * @param leaderLineLength The number of bins on each side of a line to keep
  * @param tms if true, the Y axis for tile coordinates only is flipped
  */
class LeaderLineProjectionStageOne(zoomLevels: Seq[Int],
                                   leaderLineLength: Int,
                                   min: (Double, Double),
                                   max: (Double, Double),
                                   tms: Boolean
                                  )
  extends CartesianTileProjection2D[(Double, Double, Double, Double), ((Int, Int), (Int, Int))](min, max, tms) {
//  NumericProjection[(Double, Double, Double, Double), (Int, Int, Int), ((Int, Int), (Int, Int))]((min._1, min._2, min._1, min._2), (max._1, max._2, max._1, max._2))

  private val leaderLineLengthSquared = leaderLineLength * leaderLineLength
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

    val lap = line.curLongAxisPosition
    val lastBin = line.longAxisValue(end)
    var nextBoundary = getNextTileBoundary(lap, maxBin, direction)
    if (nextBoundary * direction > lastBin * direction) {
      points += end
    } else {
      val lastBoundary = getNextTileBoundary(lastBin, maxBin, direction) - maxBin * direction
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
    if (coordinates.isEmpty) {
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
        val boundaryBins: Seq[(Int, Int)] =
          if (line2point.totalLength < 2 * leaderLineLength) {
            boundaryPointsToEnd(line2point, endUBin, maxLABins)
          } else {
            // Find first half points
            val startLeaderEnd = {
              val skipPoint = line2point.longAxisValue(line2point.skipToDistance(startUBin, leaderLineLength))
              line2point.reset()
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
        val bbb = bba.map(boundaryUBin => universalBinIndexToTileIndex(level, boundaryUBin, realMaxBin)._1)
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

/**
  * This class takes tiles and the lines that intersect them, and on a tile-by-tile basis, projects those lines into
  * individual bin values.  This is the second stage
  *
  * Note that while we inherit from CartesianTileProjection2D, we don't actually care about its projection functions,
  * just its binning functions - so we just pass in some random bounds.
  *
  * @param _tms if true, the Y axis for tile coordinates only is flipped
  * @param valueProjection a function that takes the line value option, line length, and distance from start of the current
  *                        bin, and returns Some(a value option) for that bin - or None if that bin shouldn't get included
  *                        in the resultant dataset.
  * @tparam T The value type associated with each line
  */
class LeaderLineProjectionStageTwo[T] (_tms: Boolean, valueProjection: (Option[T], Double, Double) => Option[Option[T]] = (t: Option[T], l: Double, d: Double) => Some(t))
//  extends SpreadingFunction[(Int, Int, Int), (Int, Int, Int, Int), T]
    extends CartesianBinning
{
  override protected def tms: Boolean = _tms
  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coords the visualization-space coordinates
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @param value  the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  // override
  def spread(coords: Seq[((Int, Int, Int), (Int, Int, Int, Int))], maxBin: (Int, Int), value: Option[T]): Seq[((Int, Int, Int), (Int, Int), Option[T])] = {
    coords.flatMap { case (tileCoordinates, endPoints) =>
      val (uxStart, uyStart, uxEnd, uyEnd) = endPoints
      val lineStart = (uxStart, uyStart)
      val uBounds = universalBinTileBounds(tileCoordinates, maxBin)
      val line = new LineToPoints((uxStart, uyStart), (uxEnd, uyEnd))

      val tileStart = if (line.increasing) uBounds._1 else uBounds._2
      val level = tileCoordinates._1

      line.skipTo(line.longAxisValue(tileStart))

      line.next(line.longAxisValue(maxBin) + 1).flatMap{uBin =>
        import Line.intPointToDoublePoint
        val (tile, bin) = universalBinIndexToTileIndex(level, uBin, maxBin)
        assert(tile == tileCoordinates)
        valueProjection(value, line.totalLength, Line.distance(uBin, lineStart))
          .map(binValue => (tileCoordinates, bin, binValue))
      }
    }
  }
}