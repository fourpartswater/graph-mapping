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
package software.uncharted.graphing.export

import com.typesafe.config.Config
import software.uncharted.xdata.sparkpipe.config.ConfigParser
import scala.util.Try

case class ExportConfig(source: String,
                         output: String,
                         delimiter: String,
                         maxLevel: Int)

object ExportConfigParser extends ConfigParser {

  val SECTION_KEY = "export"
  val SOURCE = "source"
  val OUTPUT = "output"
  val DELIMITER = "delimiter"
  val MAX_LEVEL = "max-level"

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
