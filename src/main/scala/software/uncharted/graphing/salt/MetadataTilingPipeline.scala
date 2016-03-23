package software.uncharted.graphing.salt

import com.oculusinfo.tilegen.datasets.LineDrawingType
import com.oculusinfo.tilegen.pipeline.OperationType
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.graphing.tiling.EdgeTilingPipelineApp
import software.uncharted.graphing.utilities.ArgumentParser
import software.uncharted.sparkpipe.Pipe

class MetadataTilingPipeline {
  def main(args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    val argParser = new ArgumentParser(args)

    val sc = new SparkContext(new SparkConf().setAppName("Edge Tiling Pipeline"))
    val sqlc = new SQLContext(sc)

    val base = argParser.getStringOption("base", "The base location of graph layout information", None).get
    val levels = argParser.getIntSeq("levels",
      """The number of levels per hierarchy level.  These will be applied in reverse order -
        | so that a value of "4,3,2" means that hierarchy level 2 will be used for tiling
        | levels 0-3, hierarcy level 1 will be used for tiling levels 4-6, and hierarchy
        | level 0 will be used for tiling levels 7 and 8.""".stripMargin, None)

    val hbaseConfigFiles = argParser.getStrings("hbaseConfig",
      "Configuration files with which to initialize HBase.  Multiple instances are permitted")
    val tableName = argParser.getStringOption("name", "The name of the node tile set to produce", None).get
    val familyName = argParser.getString("column", "The column into which to write tiles", "tileData")
    val qualifierName = argParser.getString("column-qualifier", "A qualifier to use on the tile column when writing tiles", "")


    // Initialize HBase and our table
    import GraphTilingOperations._
    val hbaseConfiguration = getHBaseConfiguration(hbaseConfigFiles)
    initializeHBaseTable(getHBaseAdmin(hbaseConfiguration), tableName, familyName)

    // Get the tiling levels corresponding to each hierarchy level
    val clusterAndGraphLevels = levels.scanLeft(0)(_ + _).sliding(2).map(bounds => (bounds(0), bounds(1) - 1)).toList.reverse.zipWithIndex.reverse

    // calculate and save our tiles
    clusterAndGraphLevels.foreach { case ((minT, maxT), g) =>
      tileHierarchyLevel(sqlc)(base, g, minT to maxT, tableName, familyName, qualifierName, hbaseConfiguration)
    }

    sc.stop()
  }

  def tileHierarchyLevel(sqlc: SQLContext)(
                         path: String,
                         hierarchyLevel: Int,
                         zoomLevels: Seq[Int],
                         tableName: String,
                         familyName: String,
                         qualifierName: String,
                         hbaseConfiguration: Configuration): Unit = {
    import GraphTilingOperations._
    import software.uncharted.sparkpipe.ops.core.rdd.{io => RDDIO}
    import RDDIO.mutateContextFcn

    val rawData = Pipe(sqlc)
      .to(RDDIO.read(path + "/level_" + hierarchyLevel))

    val nodeSchema = NodeTilingPipeline.getSchema
    val nodeData = rawData
      .to(regexFilter("^node.*"))
      .to(toDataFrame(sqlc, Map[String, String](), Some(nodeSchema)))
    // TODO: Turn into a VertexRDD

    val edgeSchema = EdgeTilingPipeline.getSchema
    val edgeData = rawData
      .to(regexFilter("^edge.*"))
      .to(toDataFrame(sqlc, Map[String, String](), Some(edgeSchema)))
    // TODO: Turn into an EdgeRDD

    Pipe(nodeData, edgeData)
    // TODO: Combine into a graph
    // TODO: Do matchEdgesWithCommunities from EdgeMatcher.scala to get community data
  }
}
