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
package software.uncharted.graphing.clustering.unithread

import com.typesafe.config.Config
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.util.Try

import scala.collection.JavaConversions._ //scalastyle:ignore

/**
  * Wrapper for all the clustering parameters needed.
  * @param inputFilename Name of the input file.
  * @param weightFilename Optional name of the weight file.
  * @param metadataFilename Optional name of the metadata file.
  * @param partitionFilename Optional name of the partition file.
  * @param minimumModularityGain A given pass stops when the modularity is increased by less than minimumModularityGain.
  * @param levelDisplay Displays the graph of level k rather than the hierachical structure.
  * @param randomize If true, will randomize processing order.
  * @param analytics Analytics aggregations to use when clustering nodes.
  * @param algorithm Algorithm modification to use when clustering.
  */
case class CommunityConfig (output: String,
                            inputFilename: String,
                            weightFilename: Option[String],
                            metadataFilename: Option[String],
                            partitionFilename: Option[String],
                            minimumModularityGain: Double,
                            levelDisplay: Int,
                            randomize: Boolean,
                            analytics: Array[CustomGraphAnalytic[_]],
                            algorithm: AlgorithmModification)

/**
  * Parser of the community clustering configuration.
  *
  * Valid properties are:
  *
  *   - `output`  - The file used as output.
  *   - `files.input` - The edge data input file.
  *   - `files.weight`  - The weight data input file.
  *   - `files.metadata`  - The meta data input file.
  *   - `files.partition` - The partition input file.
  *   - `minimum-modularity-gain` - The minimum difference in modularity necessary to perform one more level of clustering.
  *   - `level-display` - The level to output (-2 will output everything).
  *   - `keep-order`  - Maintain node order when processing to get repeatable results.
  *   - `analytics-string` - Aggregation analytics to run when clustering.
  *   - `analytics` - Aggregation analytics to run when clustering.
  *   - `algorithm.node-degree` - Limit clustering using a node degree approach.
  *   - `algorithm.community-size`  - Limit clustering using a community size approach.
  *
  *   Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *   community {
  *   output = "."
  *   files {
  *     input = "edges.bin"
  *     metadata = "metadata.bin"
  *   }
  *   algorithm {
  *     node-degree = "10"
  *   }
  *   analytics = "software.uncharted.graphing.analytics.BucketAnalytic:config/grant-analytics.conf"
  *   verbose = true
  *   level-display = -1
  * }
  */
object CommunityConfigParser extends ConfigParser {

  val SectionKey = "community"
  val Output = "output"
  val InputFilename = "files.input"
  val WeightFilename = "files.weight"
  val MetadataFilename = "files.metadata"
  val PartitionFilename = "files.partition"
  val MinimumModularityGain = "minimum-modularity-gain"
  val LevelDisplay = "level-display"
  val KeepOrder = "keep-order"
  val Analytics = "analytics-string"
  val AnalyticsSection = "analytics"
  val NodeDegree = "algorithm.node-degree"
  val CommunitySize = "algorithm.community-size"

  private val AnalyticClass = "class"
  private val AnalyticConfig = "config"

  /**
    * Parse the community clustering configuration into the wrapper class.
 *
    * @param config Configuration values to use when clustering.
    * @return The parsed configuration.
    */
  def parse(config: Config): Try[CommunityConfig] = {
    Try {
      val section = config.getConfig(SectionKey)

      // Parse objects needed for configuration.
      // Analytics should probably be setup to have a subsection and should be iterated over.
      var analytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      if (section.hasPath(Analytics)) {
        section.getString(Analytics).split(",").foreach(a => {
          val aSplit = a.split(":")
          analytics += CustomGraphAnalytic(aSplit(0), if (aSplit.length > 1) aSplit(1) else "")
        })
      } else if (section.hasPath(AnalyticsSection)) {
        val configs = section.getConfigList(AnalyticsSection)
        analytics = configs.map(c => {
          CustomGraphAnalytic(
            c.getString(AnalyticClass),
            if  (c.hasPath(AnalyticConfig)) Some(c.getConfig(AnalyticConfig)) else None
          )
        })
      }

      var algorithm: AlgorithmModification = new BaselineAlgorithm
      val nd = getStringOption(section, NodeDegree)
      val cs = getStringOption(section, CommunitySize)
      if (nd.isDefined) {
        val parameter = nd.get
        if (parameter.contains(",")) {
          algorithm = new UnlinkedNodeDegreeAlgorithm(parameter.split(",").map(_.toLong): _*)
        } else {
          algorithm = new NodeDegreeAlgorithm(parameter.toInt)
        }
      } else if (cs.isDefined) {
        algorithm = new CommunitySizeAlgorithm(cs.get.toInt)
      }

      CommunityConfig(
        section.getString(Output),
        section.getString(InputFilename),
        getStringOption(section, WeightFilename),
        getStringOption(section, MetadataFilename),
        getStringOption(section, PartitionFilename),
        section.getDouble(MinimumModularityGain),
        section.getInt(LevelDisplay),
        !section.getBoolean(KeepOrder),
        analytics.toArray,
        algorithm)
    }
  }
}
