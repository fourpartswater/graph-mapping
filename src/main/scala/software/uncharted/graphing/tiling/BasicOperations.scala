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
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.ClassTag

import software.uncharted.sparkpipe.ops.contrib.salt.{CartesianSegmentOp}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.sparkpipe.ops.contrib.salt.BasicSaltOperations.getBounds
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.projection.numeric.CartesianProjection
import software.uncharted.salt.core.analytic.numeric.CountAggregator
import software.uncharted.salt.core.generation.request.{TileLevelRequest, TileRequest}
import software.uncharted.sparkpipe.ops.contrib.salt.ArcTypes
import org.apache.spark.sql.catalyst.expressions.Literal


/**
  * Some basic operations to facilitate easy use of pipelines
  */
object BasicOperations {
  final val TILE_SIZE_DEFAULT = 256

  // Basic operations that don't care about pipeline content type
  /**
    * A function to allow optional application within a pipe, as long as there is no type change involved
    *
    * @param optOp An optional operation; if it is defined, it is applied to the input.  If it is not defined, the
    *              input is passed through untouched.
    * @param input The input data
    * @tparam T The type of input data
    * @return The input data transformed by the given operation, if there is such an operation, or else the input
    *         data itself, if not.
    */
  def optional[T] (optOp: Option[T => T])(input: T): T =
    optOp.map(op => op(input)).getOrElse(input)



  // Basic RDD operations
  /**
    * Filter input based on a given test
    *
    * @param test The test to perform
    * @param input The input data
    * @tparam T The type of input data
    * @return The input data that matches the given test
    */
  def filter[T](test: T => Boolean)(input: RDD[T]): RDD[T] =
    input.filter(test)

  /**
    * Filter an input string RDD to only those elements that match (or don't match) a given regular expression
    *
    * @param regexStr The regular expression to match
    * @param exclude If true, filter matching entries out of the RDD; if false, filter out non-matching entries
    * @param input The input data
    * @return The input data, filtered as above.
    */
  def regexFilter (regexStr: String, exclude: Boolean = false)(input: RDD[String]): RDD[String] = {
    val regex = regexStr.r
    input.filter {
      case regex(_*) => if (exclude) false else true
      case _ => if (exclude) true else false
    }
  }

  /**
    * Just map the input data to a new form (use rdd.map, but in a pipeline)
    *
    * @param fcn The transformation function
    * @param input The input data
    * @tparam S The input type
    * @tparam T The output type
    * @return The input data, transformed
    */
  def map[S, T: ClassTag](fcn: S => T)(input: RDD[S]): RDD[T] = input.map(fcn)

  // Basic Dataframe operations
  def filterA (condition: Column)(input: DataFrame): DataFrame =
    input.filter(condition)

  /**
    * Tile a dataset using a cartesian projection and a simple count aggregation
    *
    * @param xCol The column in which to find the X coordinate of the data
    * @param yCol The column in which to find the Y coordinate of the data
    * @param levels The levels to tile
    * @param boundsOpt The data bounds (minX, maxX, minY, maxY), or None to auto-detect data bounds
    * @param tileSize The size, in bins, of one output tile
    * @param input The input data
    * @return An RDD of tiles
    */
  def cartesianTiling (xCol: String, yCol: String, vCol: String, levels: Seq[Int],
                       boundsOpt: Option[(Double, Double, Double, Double)] = None,
                       tileSize: Int = TILE_SIZE_DEFAULT)(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, Double]] = {
    val bounds = boundsOpt.getOrElse {
      val columnBounds = getBounds(xCol, yCol)(input)
      val (minX, maxX) = columnBounds(0)
      val (minY, maxY) = columnBounds(1)
      // Adjust upper bounds based on max level and bins
      val rangeX = maxX - minX
      val rangeY = maxY - minY
      val epsilon = 1.0 / ((1L << levels.max) * tileSize * 4)
      (minX, minY, maxX + rangeX * epsilon, maxY + rangeY * epsilon)
    }
    val getLevel: ((Int, Int, Int)) => Int = tileIndex => tileIndex._1
    val tileAggregation: Option[Aggregator[Double, Double, Double]] = None

    CartesianOp(
      tileSize, xCol, yCol, vCol, bounds, levels, CountAggregator, tileAggregation
    )(
      new TileLevelRequest[(Int, Int, Int)](levels, getLevel)
    )(input)
  }

  /**
    * Tile a dataset with segments using a cartesian projection
    *
    * @param x1Col The column in which to find the start X coordinate of the data
    * @param y1Col The column in which to find the start Y coordinate of the data
    * @param x2Col The column in which to find the end X coordinate of the data
    * @param y2Col The column in which to find the end Y coordinate of the data
    * @param levels The levels to tile
    * @param boundsOpt The data bounds (minX, maxX, minY, maxY), or None to auto-detect data bounds
    * @param tileSize The size, in bins, of one output tile
    * @param input The input data
    * @return An RDD of tiles
    */
  // scalastyle:off parameter.number
  def segmentTiling (x1Col: String, y1Col: String, x2Col: String, y2Col: String, levels: Seq[Int],
                     arcType: ArcTypes.Value,
                     minSegLen: Option[Int] = None,
                     maxSegLen: Option[Int] = None,
                     boundsOpt: Option[(Double, Double, Double, Double)] = None,
                     tileSize: Int = TILE_SIZE_DEFAULT)(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]] = {
    val bounds = boundsOpt.getOrElse {
      val columnBounds = getBounds(x1Col, x2Col, y1Col, y2Col)(input)
      val (minX1, maxX1) = columnBounds(0)
      val (minX2, maxX2) = columnBounds(1)
      val (minY1, maxY1) = columnBounds(2)
      val (minY2, maxY2) = columnBounds(3)

      val minX = minX1 min minX2
      val maxX = maxX1 max maxX2
      val minY = minY1 min minY2
      val maxY = maxY1 max maxY2

      // Adjust upper bounds based on max level and bins
      val rangeX = maxX - minX
      val rangeY = maxY - minY
      val epsilon = 1.0 / ((1L << levels.max) * tileSize * 4)
      (minX, minY, maxX + rangeX * epsilon, maxY + rangeY * epsilon)
    }

    val tileAggregation: Option[Aggregator[Double, Double, Double]] = None

    CartesianSegmentOp(
      x1Col,
      y1Col,
      x2Col,
      y2Col,
      None,
      Some(bounds),
      arcType,
      minSegLen,
      maxSegLen,
      levels.min to levels.max,
      tileSize
    )(input)
  }

  /**
    * Add a column containing a constant value on every row.
    *
    * @param countColumnName The name of the column to use.  The caller is responsible for making sure this name is
    *                        unique in the columns of the DataFrame
    * @param constantValue The value to use for the contents of the new constant column
    * @param input The DataFrame to which to add the constant column
    * @return A new DataFrame, with the old data, plus a new constant column
    */
  def addConstantColumn (countColumnName: String, constantValue: Int)(input: DataFrame): DataFrame = {
    input.withColumn(countColumnName, new Column(Literal(constantValue)))
  }
}
