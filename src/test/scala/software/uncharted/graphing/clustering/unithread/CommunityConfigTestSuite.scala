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

class CommunityConfigTestSuite extends FunSuite {
  test("Test parsing of configuration for community step") {
    val config = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/community-defaults.conf")).bufferedReader()).resolve()
    val configParsed = CommunityConfigParser.parse(config).get
    assert(configParsed.inputFilename == "edges.bin")
    assert(configParsed.metadataFilename.get == "metadata.bin")
    assert(configParsed.levelDisplay == -1)
    assert(configParsed.analytics.length == 2)
    assert(configParsed.epsilon == 0.03)
  }
}
