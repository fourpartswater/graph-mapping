package software.uncharted.graphing.salt



import scala.collection.mutable.{Buffer => MutableBuffer}
import software.uncharted.graphing.geometry._
import software.uncharted.salt.core.spreading.SpreadingFunction



/**
  * A line projection that projects straight from lines to raster bins in one pass.
  *
  * @param zoomLevels The zoom levels onto which to project
  * @param min The minimum coordinates of the data space
  * @param max The maximum coordinates of the data space
  * @param minLengthOpt The minimum length of line (in bins) to project
  * @param maxLengthOpt The maximum length of line (in bins) to project
  * @param tms if true, the Y axis for tile coordinates only is flipped     *
  */
class SimpleLineProjection (zoomLevels: Seq[Int],
                            min: (Double, Double),
                            max: (Double, Double),
                            minLengthOpt: Option[Int] = None,
                            maxLengthOpt: Option[Int] = None,
                            tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)] (min, max, tms)
{
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates the endpoints of a line (x0, y0, x1, y1)
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return All the bins representing the given line if the given source row is within the bounds of the
    *         viz. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)

        val line2point = new LineToPoints(startUBin, endUBin)

        if (minLengthOpt.map(minLength => line2point.totalLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => line2point.totalLength <= maxLength).getOrElse(true)) {
          Some(line2point.rest().map { uBin => universalBinIndexToTileIndex(level, uBin, maxBin) }.toSeq)
        } else {
          None
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }
}



/**
  * A line projection that projects straight from lines to raster bins in one pass, emitting only a leader of
  * predefined length on each end of the line.
  *
  * @param zoomLevels The zoom levels onto which to project
  * @param min The minimum coordinates of the data space
  * @param max The maximum coordinates of the data space
  * @param leaderLineLength The number of bins to keep near each end of the line
  * @param minLengthOpt The minimum length of line (in bins) to project
  * @param maxLengthOpt The maximum length of line (in bins) to project
  * @param tms if true, the Y axis for tile coordinates only is flipped     *
  */
class SimpleLeaderLineProjection (zoomLevels: Seq[Int],
                                  min: (Double, Double),
                                  max: (Double, Double),
                                  leaderLineLength: Int = 1024,
                                  minLengthOpt: Option[Int] = None,
                                  maxLengthOpt: Option[Int] = None,
                                  tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)] (min, max, tms)
{
  private val leaderLineLengthSquared = leaderLineLength * leaderLineLength

  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates The data-space coordinate
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return A series of tile coordinate/bin index pairs if the given source row is within the bounds of the viz.
    *         None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)

        val line2point = new LineToPoints(startUBin, endUBin)

        if (minLengthOpt.map(minLength => line2point.totalLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => line2point.totalLength <= maxLength).getOrElse(true)) {
          val uBins: Seq[(Int, Int)] =
            if (line2point.totalLength > 2 * leaderLineLength) {
              val startVal = line2point.longAxisValue(startUBin)
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
              val endLeaderStart = {
                line2point.reset()
                var p = line2point.skipToDistance(endUBin, leaderLineLength + 1)
                while (Line.distanceSquared(p, endUBin) > leaderLineLengthSquared)
                  p = line2point.next()
                p
              }

              line2point.reset()
              val firstHalf =
                if (line2point.increasing) line2point.next(line2point.longAxisValue(startLeaderEnd) - startVal + 1)
                else line2point.next(startVal - line2point.longAxisValue(startLeaderEnd) + 1)
              line2point.reset()
              line2point.skipTo(line2point.longAxisValue(endLeaderStart) + (if (line2point.increasing) -1 else 1))
              val secondHalf = line2point.rest()

              firstHalf ++ secondHalf
            } else {
              line2point.rest()
            }

          val bins: Seq[((Int, Int, Int), (Int, Int))] = uBins.map(uBin =>
            universalBinIndexToTileIndex(level, uBin, maxBin)
          )

          Some(bins)
        } else {
          None: Option[Seq[((Int, Int, Int), (Int, Int))]]
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }
}


/**
  * Fade the ends of a line (as produced by the SimpleLeaderLineProjection) so they are bright near the endpoints,
  * and dull near the middle.
  */
class FadingSpreadingFunction (leaderLineLength: Int, maxBin: (Int, Int), _tms: Boolean)
  extends SpreadingFunction[(Int, Int, Int), (Int, Int), Double]
  with CartesianBinning
{
  override protected def tms: Boolean = _tms

  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coordsTraversable the visualization-space coordinates
    * @param value  the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  override def spread(coordsTraversable: Traversable[((Int, Int, Int), (Int, Int))],
                      value: Option[Double]): Traversable[((Int, Int, Int), (Int, Int), Option[Double])] = {
    val coords = coordsTraversable.toSeq
    val n = coords.length
    val halfWay = n / 2

    val all =
      if (n < 2 * leaderLineLength) true
      else {
        // Figure out if the midpoints are more than one bin apart
        val midBinLeft = coords(halfWay - 1)
        val midBinRight = coords(halfWay)
        val midUBinLeft = tileBinIndexToUniversalBinIndex(midBinLeft._1, midBinLeft._2, maxBin)
        val midUBinRight = tileBinIndexToUniversalBinIndex(midBinRight._1, midBinRight._2, maxBin)
        (midUBinLeft._1 - midUBinRight._1).abs < 2 && (midUBinLeft._2 - midUBinRight._2).abs < 2
      }

    if (all) {
      // No gaps, so just use all points without scaling
      coords.map { case (tile, bin) => (tile, bin, value) }
    } else {
      // Gap in the middle; scale points according to their distance from the end
      var i = 0
      coords.map { case (tile, bin) =>
        val scale =
          if (i < halfWay) (halfWay - i).toDouble / halfWay
          else (i - halfWay + 1).toDouble / halfWay
        i += 1

        (tile, bin, value.map(v => v * scale))
      }
    }
  }
}



/**
  * A line projection that projects straight from arcs to raster bins in one pass.  All arcs are drawn with the same
  * curvature, and go clockwise from source to destination.
  *
  * @param zoomLevels The zoom levels onto which to project
  * @param min The minimum coordinates of the data space
  * @param max The maximum coordinates of the data space
  * @param arcLength The curvature of the arcs drawn, in radians
  * @param minLengthOpt The minimum length of line (in bins) to project
  * @param maxLengthOpt The maximum length of line (in bins) to project
  * @param tms if true, the Y axis for tile coordinates only is flipped     *
  */
class SimpleArcProjection (zoomLevels: Seq[Int],
                           min: (Double, Double),
                           max: (Double, Double),
                           arcLength: Double = math.Pi / 3,
                           minLengthOpt: Option[Int] = Some(4),
                           maxLengthOpt: Option[Int] = Some(1024),
                           tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)](min, max, tms)
{
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates     the data-space coordinate
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return An optional sequence of points on the arc between the input coordinates, if the given input coordinates
    *         are within the bounds of the viz. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    import Line.intPointToDoublePoint

    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)
        val chordLength = Line.distance(startUBin, endUBin)


        if (minLengthOpt.map(minLength => chordLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => chordLength <= maxLength).getOrElse(true)) {
          import DoubleTuple._
          val arcToPoints = new ArcBinner(startUBin, endUBin, arcLength, true)
          Some(arcToPoints.remaining.map(uBin =>
            universalBinIndexToTileIndex(level, uBin, maxBin)
          ).toSeq)
        } else {
          None
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }
}



class SimpleLeaderArcProjection (zoomLevels: Seq[Int],
                                 min: (Double, Double),
                                 max: (Double, Double),
                                 leaderLength: Int,
                                 arcLength: Double = math.Pi / 3,
                                 minLengthOpt: Option[Int] = Some(4),
                                 maxLengthOpt: Option[Int] = Some(1024),
                                 tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)](min, max, tms)
{
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates The endpoints of an arc (x0, y0, x1, y1)
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return The bins near the endpoins of the given arc, if within data space bounds. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    import Line.intPointToDoublePoint

    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)
        val chordLength = Line.distance(startUBin, endUBin)


        if (minLengthOpt.map(minLength => chordLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => chordLength <= maxLength).getOrElse(true)) {
          Some {
            val binner = new ArcBinner(startUBin, endUBin, arcLength, clockwise = true)
            val center = ArcBinner.getArcCenter(startUBin, endUBin, arcLength, clockwise = true)
            val radius = ArcBinner.getArcRadius(startUBin, endUBin, arcLength)
            val leaderArcLength = ArcBinner.getArcLength(radius, leaderLength)
            if (arcLength <= 2.0 * leaderArcLength) {
              // Short arc, use the whole thing
              binner.remaining.map(uBin => universalBinIndexToTileIndex(level, uBin, maxBin)).toSeq
            } else {
              val startLeaderBuffer = MutableBuffer[(Int, Int)]()
              binner.resetToStart()
              var next = (0, 0)
              var inBounds = true
              while (binner.hasNext && inBounds) {
                next = binner.next()
                if (Line.distance(next, startUBin) < leaderLength) {
                  inBounds = true
                  startLeaderBuffer += next
                } else {
                  inBounds = false
                }
              }

              val endLeaderBuffer = MutableBuffer[(Int, Int)]()
              binner.resetToEnd()
              var previous = (0, 0)
              inBounds = true
              while (binner.hasPrevious && inBounds) {
                previous = binner.previous()
                if (Line.distance(previous, endUBin) < leaderLength) {
                  inBounds = true
                  endLeaderBuffer += previous
                } else {
                  inBounds = false
                }
              }

              (startLeaderBuffer ++ endLeaderBuffer)
                .map(uBin => universalBinIndexToTileIndex(level, uBin, maxBin))
            }
          }
        } else {
          None: Option[Seq[((Int, Int, Int), (Int, Int))]]
        }
      }.reduce ((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }
}
