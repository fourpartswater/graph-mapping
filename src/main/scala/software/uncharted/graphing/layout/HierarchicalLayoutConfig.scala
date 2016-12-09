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
package software.uncharted.graphing.layout



import com.typesafe.config.Config
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.util.Try


/**
  * @param input The location of the input data
  * @param inputParts The number of partitions into which to break up the input
  * @param inputDelimiter The delimiter used between fields in the input data.  Default is tab-delimited.
  * @param output The location to which to output layout results
  * @param outputParts The number of partitions into which to break up the output
  * @param layoutSize The desired height and width of the total node layout region.  Default is 256.0
  * @param maxHierarchyLevel The maximum clustering level used when determining graph layout
  * @param communitySizeThreshold A threshold measuring the necessary number of internal nodes a community must have
  *                               to be laid out.  Smaller communities are ignored.  Default is 0 (i.e., no communities
  *                               ignored)
  */
case class HierarchicalLayoutConfig(input: String,
                                    inputParts: Option[Int],
                                    inputDelimiter: String,
                                    output: String,
                                    outputParts: Option[Int],
                                    layoutSize: Double,
                                    maxHierarchyLevel: Int,
                                    communitySizeThreshold: Int)

object HierarchicalLayoutConfigParser extends ConfigParser {
  private val SECTION_KEY = "layout"
  private val INPUT_LOCATION_KEY = "input.location"
  private val INPUT_PARTS_KEY = "input.parts"
  private val INPUT_DELIMITER_KEY = "input.delimiter"
  private val OUTPUT_LOCATION_KEY = "output.location"
  private val OUTPUT_PARTS_KEY = "output.parts"
  private val LAYOUT_SIZE_KEY = "range"
  private val MAX_HIERARCHY_LEVEL_KEY = "max-level"
  private val COMMUNITY_SIZE_THRESHOLD_KEY = "community-size-threshold"

  private val defaultInputDelimiter = "\t"
  private val defaultLayoutSize = 256.0
  private val defaultCommunitySizeThreshold = 0

  def parse(config: Config): Try[HierarchicalLayoutConfig] = {
    Try {
      val section = config.getConfig(SECTION_KEY)

      HierarchicalLayoutConfig(
        section.getString(INPUT_LOCATION_KEY),
        getIntOption(section, INPUT_PARTS_KEY),
        getString(section, INPUT_DELIMITER_KEY, defaultInputDelimiter),
        section.getString(OUTPUT_LOCATION_KEY),
        getIntOption(section, OUTPUT_PARTS_KEY),
        getDouble(section, LAYOUT_SIZE_KEY, defaultLayoutSize),
        section.getInt(MAX_HIERARCHY_LEVEL_KEY),
        getInt(section, COMMUNITY_SIZE_THRESHOLD_KEY, defaultCommunitySizeThreshold)
      )
    }
  }
}
