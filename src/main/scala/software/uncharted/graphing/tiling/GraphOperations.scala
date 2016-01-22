package software.uncharted.graphing.tiling

import com.oculusinfo.binning.{BinIndex, TileIndex}
import com.oculusinfo.tilegen.datasets._
import com.oculusinfo.tilegen.pipeline.OperationType.OperationType
import com.oculusinfo.tilegen.pipeline._
import com.oculusinfo.tilegen.pipeline.OperationType._
import com.oculusinfo.tilegen.tiling.{StandardBinningFunctions, TileIO, LocalTileIO, HBaseTileIO}
import com.oculusinfo.tilegen.util.KeyValueArgumentSource
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.StringType

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
    println("\n\n\n" + message + input.srdd.count + "\n\n\n")
    input
  }

  def filterByRowTypeOp (intraCommunityEdges: Boolean, interCommunityEdges: Boolean, edgeTypeColumn: String)(input: PipelineData): PipelineData = {
    if (intraCommunityEdges && interCommunityEdges) input
    else if (intraCommunityEdges) {
      val c = new Column(edgeTypeColumn)
      PipelineData(input.sqlContext, input.srdd.select(c === 0))
    } else if (interCommunityEdges) {
      val c = new Column(edgeTypeColumn)
      PipelineData(input.sqlContext, input.srdd.select(c === 1))
    } else {
      throw new IllegalArgumentException("At least one of inter-community edges and intra-community edges must be chosen")
    }
  }

  //  def regexFilterOp (column:String, regex: String, exclude: Boolean = false)(input: PipelineData): PipelineData = {
  //    val test = new Column(column).rlike(regex)
  //    PipelineData(input.sqlContext, input.srdd.select(test))
  //  }

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
        Map("oculus.binning.value.type" -> "field",
          "oculus.binning.value.field" -> valueColSpec.get,
          "oculus.binning.value.valueType" -> valueColType.get,
          "oculus.binning.value.aggregation" -> operation.toString.toLowerCase,
          "oculus.binning.value.serializer" -> s"[${valueColType.get}]-a")
      case _ =>
        Map("oculus.binning.value.type" -> "count",
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
