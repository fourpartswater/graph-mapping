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

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source

trait ConfigReader {
  def readConfigArguments (configFile: Option[String], cliParser: Config => Config): Config = {
    val environmentalConfig = ConfigFactory.load()
    var configActive = cliParser(environmentalConfig)

    if (configFile.isDefined) {
      val filename = configFile.get
      val cfgFile = new File(filename)
      if (!cfgFile.exists()) {
        println(s"Config file $filename doesn't exist") //scalastyle:ignore
      } else if (!cfgFile.isFile) {
        println(s"Config file $filename is a directory, not a file") //scalastyle:ignore
      } else if (!cfgFile.canRead) {
        println(s"Can't read config file $filename") //scalastyle:ignore
      } else {
        println(s"Reading config file $cfgFile") //scalastyle:ignore
        configActive = ConfigFactory.parseReader(Source.fromFile(cfgFile).bufferedReader()).withFallback(configActive)
      }
    }

    configActive.resolve()
  }
}
