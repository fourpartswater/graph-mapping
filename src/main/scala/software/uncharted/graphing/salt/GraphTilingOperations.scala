package software.uncharted.graphing.salt



import java.lang.{Double => JavaDouble}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Put, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.io.compress.{GzipCodec, BZip2Codec}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import com.databricks.spark.csv.CsvParser

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.analytic.numeric.CountAggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.sparkpipe.ops.salt.zxy.CartesianOp



/**
  * Created by nkronenfeld on 2016-02-17.
  */
object GraphTilingOperations {
  /**
    * Implicit conversion from SQLContext to SparkContext, so RDD functions can be called with the former.
    *
    * Already submitted to sparkpipe-core; remove when that is published.
    */
  implicit def mutateContext(sqlc: SQLContext): SparkContext = sqlc.sparkContext
  /**
    * Implicit conversion from a function from SQLContext to a function from SparkContext, so RDD functions can be
    * called with the pipes in SQLContexts.
    *
    * Already submitted to sparkpipe-core; remove when that is published.
    */
  implicit def mutateContextFcn[T](fcn: SparkContext => T): SQLContext => T =
    input => fcn(input.sparkContext)

  /**
    * Read an RDD from HDFS
    *
    * Already submitted to sparkpipe-core; remove when that is published.
    */
  def read(path: String,
           format: String = "text",
           options: Map[String, String] = Map[String, String]()
          )(sc: SparkContext): RDD[String] = {
    assert("text" == format, "Only text format currently supported")
    if (options.contains("minPartitions")) {
      sc.textFile(path, options("minPartitions").trim.toInt)
    } else {
      sc.textFile(path)
    }
  }

  /**
    * Write an RDD to HDFS
    *
    * Already submitted to sparkpipe-core; remove when that is published.
    */
  def write[T](path: String,
               format: String = "text",
               options: Map[String, String] = Map[String, String]()
              )(input: RDD[T]): RDD[T] = {
    assert("text" == format, "Only text format currently supported")
    options.get("codec").map(_.trim.toLowerCase) match {
      case Some("bzip2") =>
        input.saveAsTextFile(path, classOf[BZip2Codec])
      case Some("gzip") =>
        input.saveAsTextFile(path, classOf[GzipCodec])
      case _ =>
        input.saveAsTextFile(path)
    }
    input
  }

  def filter[T](test: T => Boolean)(input: RDD[T]): RDD[T] =
    input.filter(test)

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
      setParserValue(key, value => setFcn(value.charAt(0)))

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
    * Get the bounds of two columns in a dataframe
    *
    * @param xCol The first column to examine
    * @param yCol The second column to examine
    * @param data The raw data to examine
    * @return The minimum and maximum of xCol, then the minimum and maximum of yCol.
    */
  def getBounds (xCol: String, yCol: String)(data: DataFrame): (Double, Double, Double, Double) = {
    import org.apache.spark.sql.functions._
    val minmax = data.select(min(xCol), max(xCol), min(yCol), max(yCol)).take(1)(0)
    def value (index: Int): Double = minmax(index) match {
      case d: Double => d
      case f: Float => f.toDouble
      case l: Long => l.toDouble
      case i: Int => i.toDouble
    }
    (value(0), value(1), value(2), value(3))
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
                       tileSize: Int = 256)(input: DataFrame): RDD[SeriesData[(Int, Int, Int), Double, Double]] = {
    val bounds = boundsOpt.getOrElse {
      val columnBounds = getBounds(xCol, yCol)(input)
      (columnBounds._1, columnBounds._3, columnBounds._2, columnBounds._4)
    }
    val getLevel: ((Int, Int, Int)) => Int = tileIndex => tileIndex._1
    val tileAggregation: Option[Aggregator[Double, Double, Double]] = None

    val result: RDD[SeriesData[(Int, Int, Int), Double, Double]] =
      CartesianOp(
        xCol, yCol, bounds, levels, None, CountAggregator, tileAggregation, tileSize
      )(
        new TileLevelRequest[(Int, Int, Int)](levels, getLevel)
      )(input)
    result
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
      hbaseConfiguration.addResource(configFile)
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
  def saveTiles (table: String, family: String, qualifier: String, hbaseConfiguration: Configuration)(tileData: RDD[SeriesData[(Int, Int, Int), Double, Double]]) = {
    // Convert tiles to hbase format
    val BytesPerDouble = 8
    def toByteArray (data: Seq[Double]): Array[Byte] = {
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
