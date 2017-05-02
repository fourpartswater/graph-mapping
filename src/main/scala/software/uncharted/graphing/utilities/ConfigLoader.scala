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
package software.uncharted.graphing.utilities

import com.typesafe.config.{Config, ConfigValueFactory}

/**
  * Wrapper for a configuration to be able to add or overwrite values.
  * @param config
  */
class ConfigLoader (var config: Config) {
  /**
    * Write a value to the configuration if it is provided.
    * @param value The value to write. If none is provided, then nothing is changed.
    * @param key The configuration key being updated.
    * @return A new configuration that includes the given value.
    */
  def putValue(value: Option[Object], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }

  /**
    * Write an integer value to the configuration if it is provided.
    * @param value The value to write. If none is provided, then nothing is changed.
    * @param key The configuration key being updated.
    * @return A new configuration that includes the given value.
    */
  def putIntValue(value: Option[Int], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }

  /**
    * Write a double value to the configuration if it is provided.
    * @param value The value to write. If none is provided, then nothing is changed.
    * @param key The configuration key being updated.
    * @return A new configuration that includes the given value.
    */
  def putDoubleValue(value: Option[Double], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }

  /**
    * Write a boolean value to the configuration if it is provided.
    * @param value The value to write. If none is provided, then nothing is changed.
    * @param key The configuration key being updated.
    * @return A new configuration that includes the given value.
    */
  def putBooleanValue(value: Option[Boolean], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value.get))
    }

    config
  }
}
