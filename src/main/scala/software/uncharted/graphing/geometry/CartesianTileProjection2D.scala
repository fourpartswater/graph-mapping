package software.uncharted.graphing.geometry

import software.uncharted.salt.core.projection.Projection


trait CartesianBinning {
  protected def tms: Boolean

  /**
    * Change from a (tile, bin) coordinate to a (universal bin) coordinate
    *
    * Generally, the upper left corner is taken as (0, 0).  If TMS is specified, then the tile Y coordinate is
    * flipped (i.e., lower left is (0, 0)), but the bin coordinates (both tile-bin and universal-bin) are not.
    *
    * @param tile the tile coordinate
    * @param bin the bin coordinate
    * @param maxBin the maximum bin index within each tile
    * @return The universal bin coordinate of the target cell, with (0, 0) being the upper left corner of the whole
    *         space
    */
  protected def tileBinIndexToUniversalBinIndex(tile: (Int, Int, Int),
                                                bin: (Int, Int),
                                                maxBin: (Int, Int)): (Int, Int) = {
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
    * @return The tile and bin at the given level of the given universal bin
    */
  protected def universalBinIndexToTileIndex(z: Int,
                                             universalBin: (Int, Int),
                                             maxBin: (Int, Int)) = {
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

  /**
    * Get the universal bin bounds of a given tile
    *
    * @param tile The desired tile
    * @param maxBin the maximum bin index within each tile
    * @return The minimum x and y universal bin coordinates within the tile, and the maximum.
    */
  protected def universalBinTileBounds (tile: (Int, Int, Int), maxBin: (Int,Int)): ((Int, Int),(Int, Int)) = {
    if (tms) {
      val ul = tileBinIndexToUniversalBinIndex(tile, (0, 0), maxBin)
      val lr = tileBinIndexToUniversalBinIndex(tile, maxBin, maxBin)
      (ul, lr)
    } else {
      val ul = tileBinIndexToUniversalBinIndex(tile, (0, 0), maxBin)
      val lr = tileBinIndexToUniversalBinIndex(tile, maxBin, maxBin)
      (ul, lr)
    }
  }
}

/**
  * All functions needed to project a 2-d cartesian space into a tile space, and back
  */
abstract class CartesianTileProjection2D[DC, BC] (min: (Double, Double), max: (Double, Double), _tms: Boolean)
  extends Projection[DC, (Int, Int, Int), BC] with CartesianBinning
{
  assert(max._1 > min._1)
  assert(max._2 > min._2)

  override def tms: Boolean = _tms

  // The X and Y range of our bounds; simple calculation, but we'll use it a lot.
  private val range = (max._1 - min._1, max._2 - min._2)

  // Translate an input coordinate into [0, 1) x [0, 1)
  protected def translateAndScale (x: Double, y: Double): (Double, Double) =
    ((x - min._1) / range._1, (y - min._2) / range._2)

  /** Translate from a point in the range [0, 1) x [0, 1) into universal bin coordinates at the given level */
  protected def scaledToUniversalBin (scaledPoint: (Double, Double), level: Int, maxBin: (Int, Int)): (Int, Int) = {
    val levelScale = 1L << level
    val levelX = scaledPoint._1 * levelScale
    val levelY = scaledPoint._2 * levelScale
    val tileX = levelX.floor.toInt
    val tileY = levelY.floor.toInt
    val binX = ((levelX - tileX) * (maxBin._1 + 1)).floor.toInt
    val binY = ((levelY - tileY) * (maxBin._2 + 1)).floor.toInt
    tileBinIndexToUniversalBinIndex((level, tileX, tileY), (binX, binY), maxBin)
  }
}
