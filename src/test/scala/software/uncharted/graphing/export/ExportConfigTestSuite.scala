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

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class ExportConfigTestSuite extends FunSuite {
  test("Test parsing of configuration for export step") {
    val config = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/export-defaults.conf")).bufferedReader()).resolve()
    val configParsed = ExportConfigParser.parse(config).get
    assert(configParsed.output == "output/nodes")
    assert(configParsed.maxLevel == 6)
    assert(configParsed.delimiter == "\t")
    assert(configParsed.source == "source/layout")
  }
}
