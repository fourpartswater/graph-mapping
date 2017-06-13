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

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class ConvertConfigTestSuite extends FunSuite {
  test("Test parsing of configuration for convert step") {
    val config = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/convert-defaults.conf")).bufferedReader()).resolve()
    val configParsed = ConvertConfigParser.parse(config).get
    assert(configParsed.edgeInputFilename == "edges")
    assert(configParsed.edgeOutputFilename == "edges.bin")
    assert(configParsed.srcNodeColumn == 0)
    assert(configParsed.dstNodeColumn == 1)
    assert(configParsed.metaOutputFilename.get == "metadata.bin")
    assert(configParsed.nodeInputFilename.get == "nodes")
    assert(configParsed.nodeColumn == 1)
    assert(configParsed.metaColumn == 0)
    assert(configParsed.nodeAnalytics.length == 2)
  }
}
