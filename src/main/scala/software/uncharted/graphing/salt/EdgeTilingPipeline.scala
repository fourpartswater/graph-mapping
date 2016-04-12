package software.uncharted.graphing.salt

import com.oculusinfo.tilegen.datasets.LineDrawingType
import com.oculusinfo.tilegen.pipeline.OperationType
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Column, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.graphing.salt.GraphTilingOperations._
import software.uncharted.graphing.utilities.ArgumentParser
import software.uncharted.sparkpipe.Pipe

import scala.collection.immutable.Range.Inclusive

object EdgeTilingPipeline {

  def main(args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    val argParser = new ArgumentParser(args)

    val sc = new SparkContext(new SparkConf().setAppName("Edge Tiling Pipeline"))
    val sqlc = new SQLContext(sc)


    val base             = argParser.getStringOption("base", "The base location of graph layout information", None).get
    val levels           = argParser.getIntSeq("levels",
      """The number of levels per hierarchy level.  These will be applied in reverse order -
        | so that a value of "4,3,2" means that hierarchy level 2 will be used for tiling
        | levels 0-3, hierarcy level 1 will be used for tiling levels 4-6, and hierarchy
        | level 0 will be used for tiling levels 7 and 8.""".stripMargin, None)
    val lineType = argParser.getStringOption("lineType", """The type of line to draw (leader, arc, or line)""", None).map(_.toLowerCase.trim match {
      case "leaderline" => ArcTypes.LeaderLine
      case "line" => ArcTypes.FullLine
      case "leaderarc" => ArcTypes.LeaderArc
      case "arc" => ArcTypes.FullArc
      case lt => throw new IllegalArgumentException("Illegal line type "+lt)
    })
    val edgeType = argParser.getString("edgeType", "The type of edge to plot.  Case-insensitive, possible values are intra, inter, or all.  Default is all.", "all").toLowerCase.trim match {
      case "inter" => Some(1)
      case "intra" => Some(0)
      case _ => None
    }
    val minSegLen = argParser.getIntOption("minLength", """The minimum segment length to draw (used when lineType="line" or "arc")""", None)
    val maxSegLen = argParser.getIntOption("maxLength", """When lineType="line" or "arc", the maximum segment length to draw.  When lineType="leader", the maximum leader length to draw""", None)

    val hbaseConfigFiles = argParser.getStrings("hbaseConfig",
      "Configuration files with which to initialize HBase.  Multiple instances are permitted")
    val tableName        = argParser.getStringOption("name", "The name of the node tile set to produce", None).get
    val familyName       = argParser.getString("column", "The column into which to write tiles", "tileData")
    val qualifierName    = argParser.getString("column-qualifier", "A qualifier to use on the tile column when writing tiles", "")

    // Initialize HBase and our table
    import GraphTilingOperations._
    val hbaseConfiguration = getHBaseConfiguration(hbaseConfigFiles)
    initializeHBaseTable(getHBaseAdmin(hbaseConfiguration), tableName, familyName)

    // Get the tiling levels corresponding to each hierarchy level
    val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds(0), bounds(1) - 1)).toList.reverse.zipWithIndex.reverse

    // calculate and save our tiles
    clusterAndGraphLevels.foreach { case ((minT, maxT), g) =>
      tileHierarchyLevel(sqlc)(base, g, minT to maxT, lineType, edgeType, minSegLen, maxSegLen, tableName, familyName, qualifierName, hbaseConfiguration)
    }

    sc.stop()
  }

  def getSchema: StructType = {
    StructType(Seq(
      StructField("fieldType", StringType),
      StructField("srcId", LongType),
      StructField("srcX", DoubleType),
      StructField("srcY", DoubleType),
      StructField("dstId", LongType),
      StructField("dstX", DoubleType),
      StructField("dstY", DoubleType),
      StructField("weight", LongType),
      StructField("isInterCommunity", IntegerType)
    ))
  }

  def tileHierarchyLevel(sqlc: SQLContext)(
                         path: String,
                         hierarchyLevel: Int,
                         zoomLevels: Seq[Int],
                         lineType: Option[ArcTypes.Value],
                         edgeType: Option[Int],
                         minSegLen: Option[Int],
                         maxSegLen: Option[Int],
                         tableName: String,
                         familyName: String,
                         qualifierName: String,
                         hbaseConfiguration: Configuration): Unit = {
    import GraphTilingOperations._
    import DebugGraphOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import RDDIO.mutateContextFcn

    val edgeFcn: Option[DataFrame => DataFrame] = edgeType.map {value =>
      filterA(new Column("isInterCommunity") === value)
    }
    Pipe(sqlc)
      .to(RDDIO.read(path + "/level_" + hierarchyLevel))
      .to(countRDDRowsOp(s"Level $hierarchyLevel raw data: "))
      .to(regexFilter("^edge.*"))
      .to(countRDDRowsOp("Edge data: "))
      .to(toDataFrame(sqlc, Map[String, String]("delimiter" -> "\t", "quote" -> null),
                      Some(getSchema)))
      .to(countDFRowsOp("Parsed data: "))
      .to(optional(edgeFcn))
      .to(countDFRowsOp("Required edges" ))
      .to(segmentTiling("srcX", "srcY", "dstX", "dstY", zoomLevels, lineType, minSegLen, maxSegLen, Some((0.0, 0.0, 256.0, 256.0))))
      .to(countRDDRowsOp("Tiles: "))
      .to(saveSparseTiles((255, 255), tableName, familyName, qualifierName, hbaseConfiguration))
      .run()
  }
}
