/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.tiling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.CartesianProjection
import software.uncharted.sparkpipe.ops.contrib.salt.{ZXYOp}

/**
  * An operation for generating a cartesian-projected
  * tile layer from a DataFrame, using Salt
  */
trait CartesianOp extends ZXYOp {
  // scalastyle:off parameter.number

  /**
    * Main tiling operation.
    *
    * @param tileSize       the size of one side of a tile, in bins
    * @param xCol           the name of the x column
    * @param yCol           the name of the y column
    * @param vCol           the name of the value column (the value for aggregation)
    * @param xYBounds       the y/X bounds of the "world" for this tileset, specified as (minX, minY, maxX, maxY)
    * @param zoomBounds     the zoom bounds of the "world" for this tileset, specified as a Seq of Integer zoom levels
    * @param binAggregator  an Aggregator which aggregates values from the ValueExtractor
    * @param tileAggregator an optional Aggregator which aggregates bin values
    */
  def apply[T, U, V, W, X](tileSize: Int = BasicOperations.TILE_SIZE_DEFAULT,
                           xCol: String,
                           yCol: String,
                           vCol: String,
                           xYBounds: (Double, Double, Double, Double),
                           zoomBounds: Seq[Int],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val projection = new CartesianProjection(zoomBounds, (xYBounds._1, xYBounds._2), (xYBounds._3, xYBounds._4))

    super.apply(projection, tileSize, xCol, yCol, vCol, binAggregator, tileAggregator)(request)(input)
  }
}
object CartesianOp extends CartesianOp
