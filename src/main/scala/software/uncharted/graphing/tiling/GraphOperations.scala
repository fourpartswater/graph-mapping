package software.uncharted.graphing.tiling

import com.oculusinfo.binning.{BinIndex, TileIndex}
import com.oculusinfo.tilegen.datasets._
import com.oculusinfo.tilegen.pipeline.OperationType.OperationType
import com.oculusinfo.tilegen.pipeline._
import com.oculusinfo.tilegen.pipeline.OperationType._
import com.oculusinfo.tilegen.tiling.{StandardBinningFunctions, TileIO, LocalTileIO, HBaseTileIO}
import com.oculusinfo.tilegen.util.KeyValueArgumentSource
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.{StructType, StringType}

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by nkronenfeld on 2016-01-19.
  */
object GraphOperations {

  import PipelineOperations._

  val DEFAULT_LINE_COLUMN = "line"

  // Implicit for converting a scala map to a java properties object
  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }

  def loadRawDataOp(path: String, partitions: Option[Int] = None)(input: PipelineData): PipelineData = {
    val sqlContext = input.sqlContext
    val context = sqlContext.sparkContext
    val rawData = partitions.map(p => context.textFile(path, p)).getOrElse(context.textFile(path))
    val schema = SchemaTypeUtilities.structSchema(SchemaTypeUtilities.schemaField(DEFAULT_LINE_COLUMN, StringType))
    val data = sqlContext.createDataFrame(rawData.map(line => Row(line)), schema)
    PipelineData(sqlContext, data)
  }

  def rawToCSVOp(argumentSource: KeyValueArgumentSource, column: String = DEFAULT_LINE_COLUMN)(input: PipelineData): PipelineData = {
    val context = input.sqlContext
    val inputStrings = input.srdd.select(column).rdd.map(row => Try(row(0))).filter(_.isSuccess).map(_.get.toString)
    val reader = new CSVReader(context, inputStrings, argumentSource)
    PipelineData(reader.sqlc, reader.asDataFrame)
  }

  def countRowsOp(message: String = "Number of rows: ")(input: PipelineData): PipelineData = {
    val count = input.srdd.count
    val top10 = input.srdd.take(10)
    val schema = input.srdd.schema

    def printRow (s: StructType, r: Row): Unit = {
      val fields = s.fields
      print("{ ")
      for (i <- 0 until fields.length) {
        val field = fields(i)
        print(field.name + ": ")
        field.dataType match {
          case st: StructType =>
            printRow(st, r(i).asInstanceOf[Row])
          case _ =>
            print(r(i).toString)
        }
        print(", ")
      }
      print("}")
    }

    println("\n\n\n")
    println(message + count)
    println("Rows:")
    top10.foreach { row =>
      printRow(schema, row)
      println
    }
    println("\n\n\n")
    input
  }

  def filterByRowTypeOp (intraCommunityEdges: Boolean, interCommunityEdges: Boolean, edgeTypeColumn: String)(input: PipelineData): PipelineData = {
    if (intraCommunityEdges && interCommunityEdges) input
    else if (intraCommunityEdges) {
      val c = new Column(edgeTypeColumn)
      PipelineData(input.sqlContext, input.srdd.filter(c === 0))
    } else if (interCommunityEdges) {
      val c = new Column(edgeTypeColumn)
      PipelineData(input.sqlContext, input.srdd.filter(c === 1))
    } else {
      throw new IllegalArgumentException("At least one of inter-community edges and intra-community edges must be chosen")
    }
  }

  /**
    * A basic heatmap tile generator that writes output to the local file system. Uses
    * TilingTaskParameters to manage tiling task arguments; the arguments map
    * will be passed through to that object, so all arguments required for its configuration should be set in the map.
    *
    * @param xColSpec Colspec denoting data x column
    * @param yColSpec Colspec denoting data y column
    * @param tilingParams Parameters to forward to the tiling task.
    * @param operation Aggregating operation.	Defaults to type Count if unspecified.
    * @param valueColSpec Colspec denoting the value column to use for the aggregating operation.	None
    *										 if the default type of Count is used.
    * @param valueColType Type to interpret colspec value as - float, double, int, long.	None if the default type
	 count is used for the operation.
    * @param bounds The bounds for the crossplot.	None indicates that bounds will be auto-generated based on input data.
    * @param input Pipeline data to tile.
    * @return Unmodified input data.
    */
  def graphHeatMapOp(xColSpec: String,
                     yColSpec: String,
                     tilingParams: TilingTaskParameters,
                     hbaseParameters: Option[HBaseParameters],
                     operation: OperationType = COUNT,
                     valueColSpec: Option[String] = None,
                     valueColType: Option[String] = None,
                     bounds: Option[Bounds] = None)
                    (input: PipelineData) = {
    val tileIO = hbaseParameters match {
      case Some(p) => new HBaseTileIO(p.zookeeperQuorum, p.zookeeperPort, p.hbaseMaster)
      case None => new LocalTileIO("avro")
    }


    val properties = Map("oculus.binning.projection.type" -> "areaofinterest")
    val boundsProps = bounds match {
      case Some(b) => Map("oculus.binning.projection.autobounds" -> "false",
        "oculus.binning.projection.minX" -> b.minX.toString,
        "oculus.binning.projection.minY" -> b.minY.toString,
        "oculus.binning.projection.maxX" -> b.maxX.toString,
        "oculus.binning.projection.maxY" -> b.maxY.toString)
      case None => Map("oculus.binning.projection.autobounds" -> "true")
    }

    graphHeatMapOpImpl(xColSpec, yColSpec, operation, valueColSpec, valueColType, tilingParams, tileIO,
      properties ++ boundsProps)(input)
  }

  private def graphHeatMapOpImpl(xColSpec: String,
                                 yColSpec: String,
                                 operation: OperationType,
                                 valueColSpec: Option[String],
                                 valueColType: Option[String],
                                 taskParameters: TilingTaskParameters,
                                 tileIO: TileIO,
                                 properties: Map[String, String])
                                (input: PipelineData) = {
    // Populate baseline args
    val args = Map(
      "oculus.binning.name" -> taskParameters.name,
      "oculus.binning.description" -> taskParameters.description,
      "oculus.binning.tileWidth" -> taskParameters.tileWidth.toString,
      "oculus.binning.tileHeight" -> taskParameters.tileHeight.toString,
      "oculus.binning.index.type" -> "cartesian",
      "oculus.binning.index.field.0" -> xColSpec,
      "oculus.binning.index.field.1" -> yColSpec)

    val valueProps = operation match {
      case SUM | MAX | MIN | MEAN =>
        Map("oculus.binning.value.type" -> "newfield",
          "oculus.binning.value.field" -> valueColSpec.get,
          "oculus.binning.value.valueType" -> valueColType.get,
          "oculus.binning.value.aggregation" -> operation.toString.toLowerCase,
          "oculus.binning.value.serializer" -> s"[${valueColType.get}]-a")
      case _ =>
        Map("oculus.binning.value.type" -> "newcount",
          "oculus.binning.value.valueType" -> "int",
          "oculus.binning.value.serializer" -> "[int]-a")
    }

    // Parse bounds and level args
    val levelsProps = createLevelsProps("oculus.binning", taskParameters.levels)

    val tableName = PipelineOperations.getOrGenTableName(input, "heatmap_op")

    val tilingTask = TilingTask(input.sqlContext, tableName, args ++ levelsProps ++ valueProps ++ properties)
    tilingTask.doTiling(tileIO)

    PipelineData(input.sqlContext, input.srdd, Option(tableName))
  }

  def segmentTilingOp(x1ColSpec: String,
                      y1ColSpec: String,
                      x2ColSpec: String,
                      y2ColSpec: String,
                      bounds: Option[Bounds],
                      tilingParams: TilingTaskParameters,
                      hbaseParameters: Option[HBaseParameters],
                      lineType: Option[LineDrawingType] = Some(LineDrawingType.Lines),
                      minimumSegmentLength: Option[Int] = Some(4),
                      maximumSegmentLength: Option[Int] = Some(1024),
                      maximumLeaderLength: Option[Int] = Some(1024),
                      operation: OperationType = COUNT,
                      valueColSpec: Option[String] = None,
                      valueColType: Option[String] = None
                     )
                     (input: PipelineData) = {
    val tileIO = hbaseParameters match {
      case Some(p) => new HBaseTileIO(p.zookeeperQuorum, p.zookeeperPort, p.hbaseMaster)
      case None => new LocalTileIO("avro")
    }

    val projection = bounds match {
      case Some(b) => Map(
        "oculus.binning.projection.type" -> "areaofinterest",
        "oculus.binning.projection.autobounds" -> "false",
        "oculus.binning.projection.minX" -> b.minX.toString,
        "oculus.binning.projection.minY" -> b.minY.toString,
        "oculus.binning.projection.maxX" -> b.maxX.toString,
        "oculus.binning.projection.maxY" -> b.maxY.toString)
      case None => Map(
        "oculus.binning.projection.type" -> "areaofinterest",
        "oculus.binning.projection.autobounds" -> "true")
    }
    segmentTilingOpImpl(
      x1ColSpec, y1ColSpec, x2ColSpec, y2ColSpec, operation, valueColSpec, valueColType,
      lineType, minimumSegmentLength, maximumSegmentLength, maximumLeaderLength,
      tilingParams, tileIO, projection)(input)
  }

  private def segmentTilingOpImpl(x1ColSpec: String,
                                  y1ColSpec: String,
                                  x2ColSpec: String,
                                  y2ColSpec: String,
                                  operation: OperationType,
                                  valueColSpec: Option[String],
                                  valueColType: Option[String],
                                  lineType: Option[LineDrawingType],
                                  minimumSegmentLength: Option[Int],
                                  maximumSegmentLength: Option[Int],
                                  maximumLeaderLength: Option[Int],
                                  taskParameters: TilingTaskParameters,
                                  tileIO: TileIO,
                                  projection: Map[String, String])
                                 (input: PipelineData) = {
    // Populate baseline args
    val args: Map[String, String] = Map(
      "oculus.binning.name" -> taskParameters.name,
      "oculus.binning.description" -> taskParameters.description,
      "oculus.binning.tileWidth" -> taskParameters.tileWidth.toString,
      "oculus.binning.tileHeight" -> taskParameters.tileHeight.toString,
      "oculus.binning.index.type" -> "cartesian",
      "oculus.binning.index.field.0" -> x1ColSpec,
      "oculus.binning.index.field.1" -> y1ColSpec,
      "oculus.binning.index.field.2" -> x2ColSpec,
      "oculus.binning.index.field.3" -> y2ColSpec)

    val valueProps = operation match {
      case SUM | MAX | MIN | MEAN =>
        Map("oculus.binning.value.type" -> "newfield",
          "oculus.binning.value.field" -> valueColSpec.get,
          "oculus.binning.value.valueType" -> valueColType.get,
          "oculus.binning.value.aggregation" -> operation.toString.toLowerCase,
          "oculus.binning.value.serializer" -> s"[${valueColType.get}]-a")
      case _ =>
        Map("oculus.binning.value.type" -> "newcount",
          "oculus.binning.value.valueType" -> "int",
          "oculus.binning.value.serializer" -> "[int]-a")
    }

    // Parse bounds and level args
    val levelsProps = createLevelsProps("oculus.binning", taskParameters.levels)

    val tableName = PipelineOperations.getOrGenTableName(input, "heatmap_op")

    val tilingTask = TilingTask(input.sqlContext, tableName, args ++ levelsProps ++ valueProps ++ projection)

    def withValueType[PT: ClassTag](task: TilingTask[PT, _, _, _]): PipelineData = {
      val (locateFcn, populateFcn): (Traversable[Int] => Seq[Any] => Traversable[(TileIndex, Array[BinIndex])],
        (TileIndex, Array[BinIndex], PT) => MutableMap[BinIndex, PT]) =
        lineType match {
          case Some(LineDrawingType.LeaderLines) =>
            (
              StandardBinningFunctions.locateLineLeaders(task.getIndexScheme, task.getTilePyramid,
                minimumSegmentLength, maximumLeaderLength.getOrElse(1024),
                task.getNumXBins, task.getNumYBins),
              StandardBinningFunctions.populateTileWithLineLeaders(maximumLeaderLength.getOrElse(1024),
                StandardScalingFunctions.identityScale)
              )

          case Some(LineDrawingType.Arcs) =>
            (
              StandardBinningFunctions.locateArcs(task.getIndexScheme, task.getTilePyramid,
                minimumSegmentLength, maximumLeaderLength,
                task.getNumXBins, task.getNumYBins),
              StandardBinningFunctions.populateTileWithArcs(maximumLeaderLength,
                StandardScalingFunctions.identityScale)
              )

          case Some(LineDrawingType.Lines) | _ =>
            (
              StandardBinningFunctions.locateLine(task.getIndexScheme, task.getTilePyramid,
                minimumSegmentLength, maximumSegmentLength, task.getNumXBins, task.getNumYBins),
              StandardBinningFunctions.populateTileWithLineSegments(StandardScalingFunctions.identityScale)
              )
        }
      task.doParameterizedTiling(tileIO, locateFcn, populateFcn)
      PipelineData(input.sqlContext, input.srdd, Option(tableName))
    }
    withValueType(tilingTask)

    PipelineData(input.sqlContext, input.srdd, Option(tableName))
  }

  implicit def allowOptionalChildren(stage: PipelineStage): OptionalChildrenExtension =
    new OptionalChildrenExtension(stage)
}

class OptionalChildrenExtension (stage: PipelineStage) {
  def addChild(childOpt: Option[PipelineStage]): PipelineStage =
    childOpt.map(child => stage.addChild(child)).getOrElse(stage)
}
