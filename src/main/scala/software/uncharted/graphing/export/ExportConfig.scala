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
package software.uncharted.graphing.export

import com.typesafe.config.Config
import software.uncharted.xdata.sparkpipe.config.ConfigParser
import scala.util.Try

/**
  * Wrapper class for all export parameters needed.
  * @param source Layout data to use as source for the export
  * @param output HDFS directory to use for output
  * @param delimiter Input delimiter
  * @param maxLevel The maximum level in the layout data
  */
case class ExportConfig(source: String,
                         output: String,
                         delimiter: String,
                         maxLevel: Int)

/**
  * Parser of the export configuration.
  *
  * Valid properties are:
  *
  *   - `source`  - Source data, which should be output from the layout task.
  *   - `output`  - Output to write to.
  *   - `delimiter` - Delimiter used in the source data.
  *   - `max-level` - Maximum level in the source data.
  *
  * Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  * export {
  *     source = "source/layout"
  *     output = "output/nodes"
  *     delimiter = "\t"
  *     max-level = 6
  * }
  */
object ExportConfigParser extends ConfigParser {

  val SECTION_KEY = "export"
  val SOURCE = "source"
  val OUTPUT = "output"
  val DELIMITER = "delimiter"
  val MAX_LEVEL = "max-level"

  /**
    * Parse the export configuration into the wrapper class.
    * @param config Configuration values to use when exporting.
    * @return The parsed configuration.
    */
  def parse(config: Config): Try[ExportConfig] = {
    Try {
      val section = config.getConfig(SECTION_KEY)

      ExportConfig(
        section.getString(SOURCE),
        section.getString(OUTPUT),
        section.getString(DELIMITER),
        section.getInt(MAX_LEVEL)
      )
    }
  }
}
