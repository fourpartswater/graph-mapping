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
        println(s"Config file $filename doesn't exist")
      } else if (!cfgFile.isFile) {
        println(s"Config file $filename is a directory, not a file")
      } else if (!cfgFile.canRead) {
        println(s"Can't read config file $filename")
      } else {
        // scalastyle:off regex
        println(s"Reading config file $cfgFile")
        // scalastyle:on regex
        configActive = ConfigFactory.parseReader(Source.fromFile(cfgFile).bufferedReader()).withFallback(configActive)
      }
    }

    configActive.resolve()
  }
}
