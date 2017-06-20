/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.tiling



import org.apache.spark.sql.{Row, DataFrame}
import software.uncharted.salt.core.analytic.Aggregator


//scalastyle:off class.type.parameter.name
trait MetadataAnalytic[ValueType, IntermediateBinType, FinalBinType, IntermediateTileType, FinalTileType] extends Serializable {
  def getValueExtractor(inputData: DataFrame): Row => Option[ValueType]
  def getBinAggregator: Aggregator[ValueType, IntermediateBinType, FinalBinType]
  def getTileAggregator: Option[Aggregator[FinalBinType, IntermediateTileType, FinalTileType]]
}
//scalastyle:on class.type.parameter.name
