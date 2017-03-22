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

import com.typesafe.config.{Config, ConfigValueFactory}

class ConfigLoader (var config: Config) {
  def putValue(value: Option[Object], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }

  def putIntValue(value: Option[Int], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }

  def putBooleanValue(value: Option[Boolean], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }
}
