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
package software.uncharted.graphing.config

import grizzled.slf4j.Logging

/**
  * A simple runnable class that takes command line parameters, reads the configuration generated from them, and
  * prints it out for debug purposes.
  */
object ConfigurationTester extends Logging {
  def main(args: Array[String]): Unit = {
    // load properties file from supplied URI
    val config = GraphConfig.getFullConfiguration(args, this.logger)

    println(config.root().render())
  }
}
