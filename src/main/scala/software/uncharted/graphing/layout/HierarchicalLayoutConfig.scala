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
package software.uncharted.graphing.layout



import com.typesafe.config.Config
import software.uncharted.contrib.tiling.config.ConfigParser

import scala.util.Try


/**
  * @param input The location of the input data
  * @param inputParts The number of partitions into which to break up the input.  Optional; if not present, the number
  *                   of partitions is chosen automatically by Spark.
  * @param inputDelimiter The delimiter used between fields in the input data.  Default is tab-delimited.
  * @param output The location to which to output layout results
  * @param outputParts The number of partitions into which to break up the output.  Optional; if not present, the
  *                    number of partitions is chosen automatically by Spark.
  * @param layoutSize The desired height and width of the total node layout region.  Default is 256.0
  * @param maxHierarchyLevel The maximum clustering level used when determining graph layout
  * @param communitySizeThreshold A threshold measuring the necessary number of internal nodes a community must have
  *                               to be laid out.  Communities with a number of internal nodes <= this number are
  *                               ignored.  This parameter is ignored on hierarchy level 0.  Default is 0 (i.e., no
  *                               communities are ignored)
  */
case class HierarchicalLayoutConfig(input: String,
                                    inputParts: Option[Int],
                                    inputDelimiter: String,
                                    output: String,
                                    outputParts: Option[Int],
                                    layoutSize: Double,
                                    maxHierarchyLevel: Int,
                                    communitySizeThreshold: Int)

/**
  * Object to parse the layout configuration.
  *
  * Valid properties are:
  *
  *   - `input.location`  - Location of the input data, which should be an output of the clustering step.
  *   - `input.parts` - The number of partitions into which to break up the input.
  *   - `input.delimmiter`  - The delimiters of the input data.
  *   - `output.location` - The location to which to output layout results.
  *   - `output.parts`  - The number of partitions into which to break up the output.
  *   - `range` - The desired height and width of the total node layout region.
  *   - `max-level` - The maximum clustering level used when determining graph layout.
  *   - `community-size-threshold`  - A threshold measuring the necessary number of internal nodes a community must have
  *                                   to be laid out.
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  layout {
  *   input {
  *     location = ${?BASE_LOCATION}/${?DATASET}/clusters
  *     parts = ${?PARTS}
  *   }
  *   output {
  *     location = ${?BASE_LOCATION}/${?DATASET}/layout
  *     parts = ${?PARTS}
  *   }
  *   max-level = ${?MAX_LEVEL}
  *  }
  */
object HierarchicalLayoutConfigParser extends ConfigParser {
  private val SectionKey = "layout"
  private val InputLocationKey = "input.location"
  private val InputPartsKey = "input.parts"
  private val InputDelimiterKey = "input.delimiter"
  private val OutputLocationKey = "output.location"
  private val OutputPartsKey = "output.parts"
  private val LayoutSizeKey = "range"
  private val MaxHierarchyLevelKey = "max-level"
  private val CommunitySizeThresholdKey = "community-size-threshold"

  private val defaultInputDelimiter = "\t"
  private val defaultLayoutSize = 256.0
  private val defaultCommunitySizeThreshold = 0

  /**
    * Parse the layout configuration.
    * @param config Complete configuration to use for layout
    * @return Parsed layout configuration
    */
  def parse(config: Config): Try[HierarchicalLayoutConfig] = {
    Try {
      val section = config.getConfig(SectionKey)

      HierarchicalLayoutConfig(
        section.getString(InputLocationKey),
        getIntOption(section, InputPartsKey),
        getString(section, InputDelimiterKey, defaultInputDelimiter),
        section.getString(OutputLocationKey),
        getIntOption(section, OutputPartsKey),
        getDouble(section, LayoutSizeKey, defaultLayoutSize),
        section.getInt(MaxHierarchyLevelKey),
        getInt(section, CommunitySizeThresholdKey, defaultCommunitySizeThreshold)
      )
    }
  }
}
