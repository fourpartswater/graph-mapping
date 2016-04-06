package software.uncharted.graphing.salt



import java.lang.{Double => JavaDouble}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Admin, Put, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, Column, DataFrame, SQLContext}

import com.databricks.spark.csv.CsvParser

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.analytic.numeric.CountAggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.sparkpipe.ops.salt.zxy.CartesianOp



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
  def cartesianTiling (xCol: String, yCol: String, levels: Seq[Int],
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
      xCol, yCol, bounds, levels, r => Some(1), CountAggregator, tileAggregation, tileSize
    )(
      new TileLevelRequest[(Int, Int, Int)](levels, getLevel)
    )(input)
  }

  def segmentTiling (x1Col: String, y1Col: String, x2Col: String, y2Col: String, levels: Seq[Int],
                     arcTypeOpt: Option[ArcTypes.Value] = None,
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
      arcTypeOpt.getOrElse(ArcTypes.LeaderLine), minSegLen, maxSegLen,
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

  /**
    * Helper function for initializing an HBase connection
    *
    * Get a configuration with which to connect to HBase
    *
    * @param hbaseConfigurationFiles A list of configuration files with which to initialize the configuration
    * @return A fully initialized configuration object
    */
  def getHBaseConfiguration (hbaseConfigurationFiles: Seq[String]): Configuration = {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfigurationFiles.foreach{configFile =>
      hbaseConfiguration.addResource(new Path(configFile))
    }
    hbaseConfiguration
  }

  /**
    * Helper function for initializing an HBase connection
    *
    * Create an HBase admin object with which to initialize tables
    *
    * @param hbaseConfiguration A configuration object specifying how to connect to HBase
    * @return An admin object with which to initialize tables
    */
  def getHBaseAdmin (hbaseConfiguration: Configuration) = {
    val hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration)
    hbaseConnection.getAdmin
  }

  /**
    * Helper function for initializing an HBase connection
    *
    * Initialize a table for writing, with a given column.
    *
    * @param hbaseAdmin An HBase admin object, capable of examining and creating tables.
    * @param table The table to create
    * @param family A column that the table must have
    */
  def initializeHBaseTable (hbaseAdmin: Admin, table: String, family: String) = {
    val tableName = TableName.valueOf(table)
    // Check if the table exists
    if (!hbaseAdmin.tableExists(tableName)) {
      // Table doesn't exist; create it.
      val tableDescriptor = new HTableDescriptor(tableName)
      val familyDescriptor = new HColumnDescriptor(family.getBytes)
      tableDescriptor.addFamily(familyDescriptor)
      hbaseAdmin.createTable(tableDescriptor)
    } else {
      // Table exists; make sure it has the given column
      val tableDescriptor = hbaseAdmin.getTableDescriptor(tableName)
      if (!tableDescriptor.hasFamily(family.getBytes())) {
        // Column isn't there; create it.
        val familyDescriptor = new HColumnDescriptor(family.getBytes)
        hbaseAdmin.addColumn(tableName, familyDescriptor)
      }
    }
  }

  /**
    * Save a tile set of simple Double-valued tiles out to HBase
    *
    * The table should be already initialized (see initializeHBaseTable, above)
    *
    * This will be superceded (I hope) by what Ahilan is writing.
    *
    * @param table The name of the table into which to save the tiles
    * @param family The family name of the column in which to save tiles
    * @param qualifier A qualifier to use with the column in which to save tiles
    * @param hbaseConfiguration A fully loaded HBase configuration object
    * @param tileData An RDD of simple double-valued tiles.
    */
  def saveTiles (table: String, family: String, qualifier: String, hbaseConfiguration: Configuration)(tileData: RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, Double]]) = {
    // Convert tiles to hbase format
    val BytesPerDouble = 8
    def toByteArray (sparseData: SparseArray[Double]): Array[Byte] = {
      val data = sparseData.seq
      val result = new Array[Byte](data.length * BytesPerDouble)
      var resultIndex = 0
      for (i <- data.indices) {
        val datum = JavaDouble.doubleToLongBits(data(i).doubleValue())
        for (i <- 0 to 7) {
          result(resultIndex) = ((datum >> (i * 8)) & 0xff).asInstanceOf[Byte]
          resultIndex += 1
        }
      }
      result
    }
    def getRowIndex (tileIndex: (Int, Int, Int)): String = {
      val (z, x, y) = tileIndex
      val digits = math.log10(1 << z).floor.toInt + 1
      ("%02d,%0"+digits+"d,%0"+digits+"d").format(z, x, y)
    }

    val familyBytes = family.getBytes
    val qualifierBytes = qualifier.getBytes

    val hbaseFormattedTiles = tileData.map { tile =>
      val data = toByteArray(tile.bins)
      val rowIndex = getRowIndex(tile.coords)
      val put = new Put(rowIndex.getBytes())
      put.addColumn(familyBytes, qualifierBytes, data)
      (new ImmutableBytesWritable, put)
    }

    // Write hbase tiles
    val jobConfiguration = new JobConf(hbaseConfiguration, this.getClass)
    jobConfiguration.setOutputFormat(classOf[TableOutputFormat])
    jobConfiguration.set(TableOutputFormat.OUTPUT_TABLE, table)

    hbaseFormattedTiles.saveAsHadoopDataset(jobConfiguration)
  }
}
