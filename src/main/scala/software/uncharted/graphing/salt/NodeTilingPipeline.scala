package software.uncharted.graphing.salt


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

import software.uncharted.graphing.utilities.ArgumentParser
import software.uncharted.sparkpipe.Pipe



object NodeTilingPipeline {
  def main(args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    val argParser = new ArgumentParser(args)

    val sc = new SparkContext(new SparkConf().setAppName("Node Tiling"))
    val sqlc = new SQLContext(sc)


    val base             = argParser.getStringOption("base", "The base location of graph layout information", None).get
    val levels           = argParser.getIntSeq("levels",
      """The number of levels per hierarchy level.  These will be applied in reverse order -
        | so that a value of "4,3,2" means that hierarchy level 2 will be used for tiling
        | levels 0-3, hierarcy level 1 will be used for tiling levels 4-6, and hierarchy
        | level 0 will be used for tiling levels 7 and 8.""".stripMargin, None)
    val hbaseConfigFiles = argParser.getStrings("hbaseConfig",
      "Configuration files with which to initialize HBase.  Multiple instances are permitted")
    val tableName        = argParser.getStringOption("name", "The name of the node tile set to produce", None).get
    val familyName       = argParser.getString("column", "The column into which to write tiles", "tileData")
    val qualifierName    = argParser.getString("column-qualifier",
      "A qualifier to use on the tile column when writing tiles", "")

    // Initialize HBase and our table
    import GraphTilingOperations._
    val hbaseConfiguration = getHBaseConfiguration(hbaseConfigFiles)
    initializeHBaseTable(getHBaseAdmin(hbaseConfiguration), tableName, familyName)

    // Get the tiling levels corresponding to each hierarchy level
    val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds(0), bounds(1) - 1)).toList.reverse.zipWithIndex.reverse

    // calculate and save our tiles
    clusterAndGraphLevels.foreach { case ((minT, maxT), g) =>
      println(s"Tiling hierarchy level $g at tile levels $minT to $maxT")
      tileHierarchyLevel(sqlc)(base, g, minT to maxT, tableName, familyName, qualifierName, hbaseConfiguration)
    }

    sc.stop()
  }

  def getSchema: StructType = {
    // This schema must match that written by HierarchicalFDLayout.saveLayoutResult (the resultNodes variable)
    // "node\t" + id + "\t" + x + "\t" + y + "\t" + radius + "\t" + parentID + "\t" + parentX + "\t" + parentY + "\t" + parentR + "\t" + numInternalNodes + "\t" + degree + "\t" + metaData
    StructType(Seq(
      StructField("fieldType", StringType),
      StructField("nodeId", LongType),
      StructField("x", DoubleType),
      StructField("y", DoubleType),
      StructField("r", DoubleType),
      StructField("parentId", LongType),
      StructField("parentX", DoubleType),
      StructField("parentY", DoubleType),
      StructField("parentR", DoubleType),
      StructField("internalNodes", LongType),
      StructField("degree", IntegerType),
      StructField("metadata", StringType)
    ))
  }

  def tileHierarchyLevel(sqlc: SQLContext)(
                         path: String,
                         hierarchyLevel: Int,
                         zoomLevels: Seq[Int],
                         tableName: String,
                         family: String,
                         qualifier: String,
                         hbaseConfiguration: Configuration
  ): Unit = {
    import GraphTilingOperations._
    import DebugGraphOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import RDDIO.mutateContextFcn

    val schema = getSchema

    val tiles = Pipe(sqlc)
      .to(RDDIO.read(path + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Level $hierarchyLevel raw data: "))
      .to(regexFilter("^node.*"))
      .to(countRDDRowsOp("Node data: "))
      .to(toDataFrame(sqlc, Map[String, String]("delimiter" -> "\t", "quote" -> null),
                      Some(schema)))
      .to(countDFRowsOp("Parsed data: "))
      .to(cartesianTiling("x", "y", zoomLevels, Some((0.0, 0.0, 256.0, 256.0))))
      .to(countRDDRowsOp("Tiles: "))
      .to(saveSparseTiles((255, 255), tableName, family, qualifier, hbaseConfiguration))
      .run()
  }
}
