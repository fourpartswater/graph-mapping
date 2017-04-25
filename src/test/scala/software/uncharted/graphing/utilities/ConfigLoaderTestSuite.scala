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
package software.uncharted.graphing.utilities

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class ConfigLoaderTestSuite extends FunSuite {
  test("Test setting values in a configuration object") {
    val config = ConfigFactory.parseReader(scala.io.Source.fromURL(getClass.getResource("/export-defaults.conf")).bufferedReader()).resolve()
    assert(config.getString("export.output") == "output/nodes")
    assert(config.getInt("export.max-level") == 6)

    val loader = new ConfigLoader(config)
    var configUpdated = loader.putIntValue(Some(1), "export.max-level")
    configUpdated = loader.putValue(Some("new output"), "export.output")
    assert(config.getString("export.output") == "output/nodes")
    assert(config.getInt("export.max-level") == 6)
    assert(configUpdated.getString("export.output") == "new output")
    assert(configUpdated.getInt("export.max-level") == 1)
  }
}
