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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import software.uncharted.graphing.utilities.{ArgumentParser, ConfigLoader, ConfigReader}
import com.typesafe.config.Config

import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.util.{Failure, Success}

//scalastyle:off multiple.string.literals
/**
  * Object to export the layout data for downstream ingestion.
  */
object ESIngestExport extends ConfigReader {
  private def applySparkConfigEntries (config: Config)(conf: SparkConf): SparkConf = {
    config.getConfig("spark")
      .entrySet()
      .asScala
      .foreach(e => conf.set(s"spark.${e.getKey}", e.getValue.unwrapped().toString))

    conf
  }

  /**
    * Parse CLI parameters into a new configuration.
    * @param config Base configuration to use.
    * @param argParser Argument parser to use to parse CLI parameters.
    * @return The configuration containing the base values & the parsed CLI parameters.
    */
  def parseArguments(config: Config, argParser: ArgumentParser): Config = {
    val loader = new ConfigLoader(config)
    loader.putValue(argParser.getStringOption("sourceLayout", "The source directory containing the graph layout data", None),
      s"${ExportConfigParser.SECTION_KEY}.${ExportConfigParser.SOURCE}")
    loader.putValue(argParser.getStringOption("output", "The output location where to save data", None),
      s"${ExportConfigParser.SECTION_KEY}.${ExportConfigParser.OUTPUT}")
    loader.putValue(argParser.getStringOption("d", "Delimiter for the source graph data. Default is tab-delimited", Some("\t")),
      s"${ExportConfigParser.SECTION_KEY}.${ExportConfigParser.DELIMITER}")
    loader.putIntValue(argParser.getIntOption("maxLevel", "Max cluster hierarchy level when the data was clustered", Some(0)),
      s"${ExportConfigParser.SECTION_KEY}.${ExportConfigParser.MAX_LEVEL}")

    loader.config
  }

  def main(args: Array[String]) {
    val argParser = new ArgumentParser(args)

    // Parse config files first.
    val configFile = argParser.getStringOption("config", "File containing configuration information.", None)
    val configComplete = readConfigArguments(configFile, c => parseArguments(c, argParser))
    val exportConfig = ExportConfigParser.parse(configComplete) match {
      case Success(s) => s
      case Failure(f) =>
        println(s"Failed to load cluster configuration properly. ${f}")
        f.printStackTrace()
        sys.exit(-1)
    }

    val session = SparkSession.builder.config(applySparkConfigEntries(configComplete)(new SparkConf().setAppName("Graph Data Extraction"))).getOrCreate()

    val fileStartTime = System.currentTimeMillis()

    val exporter = new Exporter()

    exporter.exportData(session,
      exportConfig.source,
      exportConfig.output,
      exportConfig.delimiter,
      exportConfig.maxLevel)

    val fileEndTime = System.currentTimeMillis()
    println("Finished extracting data for ES ingestion in " + ((fileEndTime - fileStartTime) / 60000.0) + " minutes")

    println(s"Data extracted and available at ${exportConfig.output}")
  }
}
//scalastyle:on multiple.string.literals
