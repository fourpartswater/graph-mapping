package software.uncharted.graphing.utilities

import com.typesafe.config.{Config, ConfigValueFactory}

class ConfigLoader (var config: Config) {
  def putValue(value: Option[Object], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value))
    }

    config
  }

  def putIntValue(value: Option[Int], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value))
    }

    config
  }

  def putBooleanValue(value: Option[Boolean], key: String): Config = {
    if (value.isDefined) {
      config = config.withValue(key, ConfigValueFactory.fromAnyRef(value))
    }

    config
  }
}
