/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
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

case class CommunityConfig (inputFilename: String,
                       weightFilename: Option[String],
                       metadataFilename: Option[String],
                       partitionFilename: Option[String],
                       epsilon: Double,
                       levelDisplay: Int,
                       k: Int,
                       verbose: Boolean,
                       randomize: Boolean,
                       analytics: Seq[CustomGraphAnalytic[_]],
                       algorithm: AlgorithmModification)

object CommunityConfigParser extends ConfigParser {

  val SECTION_KEY = "community"
  val INPUT_FILENAME = "files.input"
  val WEIGHT_FILENAME = "files.weight"
  val METADATA_FILENAME = "files.metadata"
  val PARTITION_FILENAME = "files.partition"
  val EPSILON = "algorithm.epsilon"
  val LEVEL_DISPLAY = "level-display"
  val K = "k"
  val VERBOSE = "verbose"
  val KEEP_ORDER = "keep-order"
  val ANALYTICS = "analytics"
  val NODE_DEGREE = "algorithm.node-degree"
  val COMMUNITY_SIZE = "algorithm.community-size"

  def parse(config: Config): Try[CommunityConfig] = {
    Try {
      val section = config.getConfig(SECTION_KEY)

      // Parse objects needed for configuration.
      // Analytics should probably be setup to have a subsection and should be iterated over.
      var analytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      if (section.hasPath(ANALYTICS)) {
        section.getString(ANALYTICS).split(",").foreach(a => {
          val aSplit = a.split(":")
          analytics += CustomGraphAnalytic(aSplit(0), if (aSplit.length > 1) aSplit(1) else "")
        })
      }

      var algorithm: AlgorithmModification = new BaselineAlgorithm
      val nd = getStringOption(section, NODE_DEGREE)
      val cs = getStringOption(section, COMMUNITY_SIZE)
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
        section.getString(INPUT_FILENAME),
        getStringOption(section, WEIGHT_FILENAME),
        getStringOption(section, METADATA_FILENAME),
        getStringOption(section, PARTITION_FILENAME),
        section.getDouble(EPSILON),
        section.getInt(LEVEL_DISPLAY),
        section.getInt(K),
        section.getBoolean(VERBOSE),
        !section.getBoolean(KEEP_ORDER),
        analytics,
        algorithm)
    }
  }
}
