/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.salt



import java.nio.{ByteOrder, DoubleBuffer, ByteBuffer}

import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.rdd.RDDTileGenerator
import software.uncharted.xdata.ops.salt.{ArcTypes, CartesianSegmentOp}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.language.implicitConversions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import com.databricks.spark.csv.CsvParser
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.analytic.numeric.CountAggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.{TileLevelRequest, TileRequest}
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.sparkpipe.ops.community.salt.zxy.CartesianOp



/**
  * Non-standard operations needed for graph tiling
  */
object GraphTilingOperations {
  def filter[T](test: T => Boolean)(input: RDD[T]): RDD[T] =
    input.filter(test)

  def filterA (condition: Column)(input: DataFrame): DataFrame =
    input.filter(condition)

  def regexFilter (regexStr: String, exclude: Boolean = false)(input: RDD[String]): RDD[String] = {
    val regex = regexStr.r
    input.filter {
      case regex(_*) => if (exclude) false else true
      case _ => if (exclude) true else false
    }
  }

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

  /**
    * Convert an RDD of objects that are products (e.g. case classes) into a dataframe
    *
    * @param sqlc A SQL context into which to set the dataframe
    * @param input The input data
    * @tparam T The type of input data
    * @return A dataframe representing the same data
    */
  def toDataFrame[T <: Product : TypeTag](sqlc: SQLContext)(input: RDD[T]): DataFrame = {
    sqlc.createDataFrame(input)
  }

  /**
    * Convert an RDD of strings into a dataframe
    *
    * @param sqlc A SQL context into which to set the dataframe.
    * @param settings Settings to control CSV parsing.
    * @param schemaOpt The schema into which to parse the data (if null, inferSchema must be true)
    * @param input The input data
    * @return A dataframe representing the same data, but parsed into proper typed rows.
    */
  def toDataFrame (sqlc: SQLContext, settings: Map[String, String], schemaOpt: Option[StructType])(input: RDD[String]): DataFrame = {
    val parser = new CsvParser

    // Move settings to our parser
    def setParserValue (key: String, setFcn: String => Unit): Unit =
      settings.get(key).foreach(strValue => setFcn(strValue))
    def setParserBoolean (key: String, setFcn: Boolean => Unit): Unit =
      setParserValue(key, value => setFcn(value.trim.toLowerCase.toBoolean))
    def setParserCharacter (key: String, setFcn: Character => Unit): Unit =
      setParserValue(key, value => setFcn(if (null == value) null else value.charAt(0)))

    setParserBoolean("useHeader", parser.withUseHeader(_))
    setParserBoolean("ignoreLeadingWhiteSpace", parser.withIgnoreLeadingWhiteSpace(_))
    setParserBoolean("ignoreTrailingWhiteSpace", parser.withIgnoreTrailingWhiteSpace(_))
    setParserBoolean("treatEmptyValuesAsNull", parser.withTreatEmptyValuesAsNulls(_))
    setParserBoolean("inferSchema", parser.withInferSchema(_))
    setParserCharacter("delimiter", parser.withDelimiter(_))
    setParserCharacter("quote", parser.withQuoteChar(_))
    setParserCharacter("escape", parser.withEscape(_))
    setParserCharacter("comment", parser.withComment(_))
    setParserValue("parseMode", parser.withParseMode(_))
    setParserValue("parserLib", parser.withParserLib(_))
    setParserValue("charset", parser.withCharset(_))
    setParserValue("codec", parser.withCompression(_))

    schemaOpt.map(schema => parser.withSchema(schema))

    parser.csvRdd(sqlc, input)
  }

  /**
    * Get the bounds of specified columns in a dataframe
    *
    * @param columns The columns to examine
    * @param data The raw data to examine
    * @return The minimum and maximum of xCol, then the minimum and maximum of yCol.
    */
  def getBounds (columns: String*)(data: DataFrame): Seq[(Double, Double)] = {
    import org.apache.spark.sql.functions._
    val selects = columns.flatMap(column => Seq(min(column), max(column)))
    val minMaxes = data.select(selects:_*).take(1)(0).toSeq.map(_ match {
      case d: Double => d
      case f: Float => f.toDouble
      case l: Long => l.toDouble
      case i: Int => i.toDouble
    })

    minMaxes.grouped(2).map(bounds => (bounds(0), bounds(1))).toSeq
  }

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
                       tileSize: Int = 256)(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, Double]] = {
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

  def segmentTiling (x1Col: String, y1Col: String, x2Col: String, y2Col: String, levels: Seq[Int],
                     arcType: ArcTypes.Value,
                     minSegLen: Option[Int] = None,
                     maxSegLen: Option[Int] = None,
                     boundsOpt: Option[(Double, Double, Double, Double)] = None,
                     tileSize: Int = 256)(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, Double]] = {
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

    val getLevel: ((Int, Int, Int)) => Int = tileIndex => tileIndex._1
    val tileAggregation: Option[Aggregator[Double, Double, Double]] = None

    CartesianSegmentOp(
      arcType, minSegLen, maxSegLen,
      x1Col, y1Col, x2Col, y2Col,
      bounds, (levels.min, levels.max),
      row => Some(1),
      CountAggregator,
      tileAggregation,
      tileSize
    )(
      new TileLevelRequest[(Int, Int, Int)](levels, getLevel)
    )(input)
  }

  def genericFullTilingRequest[RT, DC, TC: ClassTag, BC, T, U, V, W, X] (series: Series[RT, DC, TC, BC, T, U, V, W, X], levels: Seq[Int], getZoomLevel: TC => Int)(data: RDD[RT]): RDD[SeriesData[TC, BC, V, X]] = {
    genericTiling(series)(new TileLevelRequest[TC](levels, getZoomLevel))(data)
  }

  def genericTiling[RT, DC, TC: ClassTag, BC, T, U, V, W, X] (series: Series[RT, DC, TC, BC, T, U, V, W, X])(request: TileRequest[TC])(data: RDD[RT]): RDD[SeriesData[TC, BC, V, X]] = {
    val sc = data.sparkContext
    val generator = new RDDTileGenerator(sc)

    generator.generate(data, series, request).flatMap(t => series(t))
  }

  private val BytesPerDouble = 8
  private val BytesPerInt = 4
  private val doubleTileToByteArrayDense: SparseArray[Double] => Seq[Byte] = sparseData => {
    val data = sparseData.seq.toArray
    val byteBuffer = ByteBuffer.allocate(data.length * BytesPerDouble).order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer.asDoubleBuffer().put(DoubleBuffer.wrap(data))
    byteBuffer.array().toSeq
  }

  private val doubleTileToByteArraySparse: SparseArray[Double] => Seq[Byte] = sparseData => {
    val defaultValue = sparseData.default
    var nonDefaultCount = 0
    for (i <- 0 until sparseData.length())
      if (defaultValue != sparseData(i)) nonDefaultCount += 1

    val buffer = ByteBuffer.allocate(BytesPerInt + BytesPerDouble + nonDefaultCount * (BytesPerInt + BytesPerDouble))
      .order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(nonDefaultCount)
    buffer.putDouble(defaultValue)
    for (i <- 0 until sparseData.length()) {
      if (defaultValue != sparseData(i)) {
        buffer.putInt(i)
        buffer.putDouble(sparseData(i))
      }
    }
    buffer.array().toSeq
  }

  def serializeTiles[TC, BC, V, X] (serializationFcn: SparseArray[V] => Seq[Byte])(input: RDD[SeriesData[TC, BC, V, X]]): RDD[(TC, Seq[Byte])] = {
    input.map { tile =>
      (tile.coords, serializationFcn(tile.bins))
    }
  }

  def serializeTilesDense[TC, BC, X] (input: RDD[SeriesData[TC, BC, Double, X]]): RDD[(TC, Seq[Byte])] = {
    serializeTiles[TC, BC, Double, X](doubleTileToByteArrayDense)(input)
  }
  def serializeTilesSparse[TC, BC, X] (input: RDD[SeriesData[TC, BC, Double, X]]): RDD[(TC, Seq[Byte])] = {
    serializeTiles[TC, BC, Double, X](doubleTileToByteArraySparse)(input)
  }
}
