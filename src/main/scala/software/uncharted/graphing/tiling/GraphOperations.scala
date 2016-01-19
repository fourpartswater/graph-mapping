package software.uncharted.graphing.tiling

import com.oculusinfo.binning.{BinIndex, TileIndex}
import com.oculusinfo.tilegen.datasets.{StandardScalingFunctions, TilingTask, LineDrawingType, TilingTaskParameters}
import com.oculusinfo.tilegen.pipeline.{Bounds, PipelineOperations, PipelineData, HBaseParameters}
import com.oculusinfo.tilegen.pipeline.OperationType._
import com.oculusinfo.tilegen.tiling.{StandardBinningFunctions, TileIO, LocalTileIO, HBaseTileIO}

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.ClassTag

/**
  * Created by nkronenfeld on 2016-01-19.
  */
object GraphOperations {

  import PipelineOperations._

  // Implicit for converting a scala map to a java properties object
  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
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
}
