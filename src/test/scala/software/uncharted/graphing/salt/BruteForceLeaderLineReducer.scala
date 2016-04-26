package software.uncharted.graphing.salt



import software.uncharted.graphing.geometry.{Line, LineToPoints, CartesianTileProjection2D}



/*
 * Use Line2Points to figure out leader line expansions of lines by brute force, to use when testing more
 * refined methods.
 */
class BruteForceLeaderLineReducer (maxBin: (Int, Int),
                                   bounds: ((Double, Double), (Double, Double)),
                                   level: Int,
                                   leaderLength: Int,
                                   tms: Boolean)
  extends CartesianTileProjection2D[(Int, Int, Int), ((Int, Int), (Int, Int))](bounds._1, bounds._2, tms)
{
  def getUniversalBins (x0: Double, y0: Double, x1: Double, y1: Double) = {
    def project(x: Double, y: Double) = {
      val scale = 1 << level
      val usx = (x - bounds._1._1) / (bounds._2._1 - bounds._1._1)
      val usy = (y - bounds._1._2) / (bounds._2._2 - bounds._1._2)
      val maxy = scale * (maxBin._2 + 1)
      val sx = usx * scale * (maxBin._1 + 1)
      val sy = maxy - usy * scale * (maxBin._2 + 1)
      (sx.floor.toInt, sy.floor.toInt)
    }

    val s = project(x0, y0)
    val e = project(x1, y1)

    val line = new LineToPoints(s, e)

    import Line.intPointToDoublePoint
    import Line.distance

    line.rest().filter(p => distance(p, s) <= leaderLength || distance(p, e) <= leaderLength)
  }

  def getBins (x0: Double, y0: Double, x1: Double, y1: Double) = {
    getUniversalBins(x0, y0, x1, y1).map{uBin =>
      universalBinIndexToTileIndex(level, uBin, maxBin)
    }
  }

  def getTiles (x0: Double, y0: Double, x1: Double, y1: Double) = {
    val points = getUniversalBins(x0, y0, x1, y1)

    val closeTiles = points.map(p => universalBinIndexToTileIndex(level, p, maxBin)._1)

    closeTiles.distinct
  }

  override def project(dc: Option[(Int, Int, Int)], maxBin: ((Int, Int), (Int, Int))): Option[Seq[((Int, Int, Int), ((Int, Int), (Int, Int)))]] = None
  override def binTo1D(bin: ((Int, Int), (Int, Int)), maxBin: ((Int, Int), (Int, Int))): Int = 0
}
