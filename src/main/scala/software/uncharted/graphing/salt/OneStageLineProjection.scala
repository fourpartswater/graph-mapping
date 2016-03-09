package software.uncharted.graphing.salt

import software.uncharted.graphing.geometry.{Line, LineToPoints, CartesianTileProjection2D}

/**
  * Created by nkronenfeld on 08/03/16.
  */
class SimpleLeaderLineProjection (zoomLevels: Seq[Int],
                                  leaderLineLength: Int,
                                  min: (Double, Double),
                                  max: (Double, Double),
                                  tms: Boolean)
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
      val realMaxBin = maxBin._1

      Some(zoomLevels.flatMap { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)

        val line2point = new LineToPoints(startUBin, endUBin)

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
              line2point.reset
              var p = line2point.skipToDistance(endUBin, leaderLineLength+1)
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

            (firstHalf ++ secondHalf)
          } else {
            line2point.rest()
          }

        val bins: Seq[((Int, Int, Int), (Int, Int))] = uBins.map(uBin =>
          universalBinIndexToTileIndex(level, uBin, maxBin)
        )

        bins
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
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }
}

