package software.uncharted.graphing.salt

import java.util.Date

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame}
import software.uncharted.salt.core.analytic.Aggregator

trait MetadataAnalytic[ValueType, IntermediateBinType, FinalBinType, IntermediateTileType, FinalTileType] extends Serializable {
  def getValueExtractor(inputData: DataFrame): Row => Option[ValueType]
  def getBinAggregator: Aggregator[ValueType, IntermediateBinType, FinalBinType]
  def getTileAggregator: Option[Aggregator[FinalBinType, IntermediateTileType, FinalTileType]]
}
