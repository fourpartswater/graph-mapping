/**
  * This code is copied and translated from https://sites.google.com/site/findcommunities, then modified futher to
  * support analytics and metadata.
  *
  * This means most of it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
  * we can't distribute it without permission - though as a translation, with some optimization for readability in
  * scala, it may be a gray area.
  *
  * TThe rest is:
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
package software.uncharted.graphing.clustering.unithread



import java.io._

import com.typesafe.config.{ConfigFactory, Config}
import grizzled.slf4j.Logging

import scala.io.Source
import scala.util.{Failure, Success}

import scala.collection.mutable.{Buffer => MutableBuffer}
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.graphing.utilities.{ArgumentParser, ConfigLoader}



object Convert extends Logging {

  def readConfigArguments (configFile: Option[String]): Config = {
    val environmentalConfig = ConfigFactory.load()
    var configActive = environmentalConfig

    if (configFile.isDefined) {
      val filename = configFile.get
      val cfgFile = new File(filename)
      if (!cfgFile.exists()) {
        logger.warn(s"Config file $filename doesn't exist")
      } else if (!cfgFile.isFile) {
        logger.warn(s"Config file $filename is a directory, not a file")
      } else if (!cfgFile.canRead) {
        logger.warn(s"Can't read config file $filename")
      } else {
        // scalastyle:off regex
        println(s"Reading config file $cfgFile")
        // scalastyle:on regex
        configActive = environmentalConfig.withFallback(ConfigFactory.parseReader(Source.fromFile(cfgFile).bufferedReader()))
      }
    }

    configActive.resolve()
  }

  def parseArguments(config: Config, argParser: ArgumentParser): Config = {
    val loader = new ConfigLoader(config)
    loader.putValue(argParser.getStringOption("ie", "Edge input file", None), ConvertConfigParser.EDGE_INPUT)
    loader.putValue(argParser.getStringOption("fe", "Edge filter", None), ConvertConfigParser.EDGE_FILTER)
    loader.putValue(argParser.getStringOption("ce", "Edge separator", None), ConvertConfigParser.EDGE_SEPARATOR)
    loader.putValue(argParser.getStringOption("ae", "Edge analytics", None), ConvertConfigParser.EDGE_ANALYTIC)
    loader.putIntValue(argParser.getIntOption("s", "Edge source column", None), ConvertConfigParser.SRC_NODE_COLUMN)
    loader.putIntValue(argParser.getIntOption("d", "Edge destination column", None), ConvertConfigParser.DST_NODE_COLUMN)
    loader.putIntValue(argParser.getIntOption("w", "Edge weight column", None), ConvertConfigParser.WEIGHT_COLUMN)
    loader.putValue(argParser.getStringOption("in", "Node input file", None), ConvertConfigParser.NODE_INPUT)
    loader.putValue(argParser.getStringOption("fn", "Node filter", None), ConvertConfigParser.NODE_FILTER)
    loader.putValue(argParser.getStringOption("cn", "Node separator", None), ConvertConfigParser.NODE_SEPARATOR)
    loader.putValue(argParser.getStringOption("an", "Node analytics", None), ConvertConfigParser.NODE_ANALYTIC)
    loader.putValue(argParser.getStringOption("anc", "Node analytics parameter", None), ConvertConfigParser.NODE_ANALYTIC)
    loader.putIntValue(argParser.getIntOption("n", "Node id column", None), ConvertConfigParser.NODE_COLUMN)
    loader.putIntValue(argParser.getIntOption("m", "Node metadata column", None), ConvertConfigParser.META_COLUMN)
    loader.putValue(argParser.getStringOption("oe", "Edge output file", None), ConvertConfigParser.EDGE_OUTPUT)
    loader.putValue(argParser.getStringOption("ow", "Weight output file", None), ConvertConfigParser.WEIGHT_OUTPUT)
    loader.putValue(argParser.getStringOption("om", "Metadata output file", None), ConvertConfigParser.META_OUTPUT)
    loader.putBooleanValue(argParser.getBooleanOption("r", "Renumber nodes to be zero based", None), ConvertConfigParser.RENUMBER)

    loader.config
  }

  def main(args: Array[String]): Unit = {
    val argParser = new ArgumentParser(args)

    // Parse config files first.
    val configFile = argParser.getStringOption("config", "File containing configuration information.", None)
    val config = readConfigArguments(configFile)

    // Apply the rest of the arguments to the config.
    val configComplete = parseArguments(config, argParser)
    val convertConfig = ConvertConfigParser.parse(configComplete) match {
      case Success(s) => s
      case Failure(f) =>
        error("Failed to load convert configuration properly.")
        sys.exit(-1)
    }

    // Use the config to pass all the necessary parameters.
    var g = GraphEdges(convertConfig.edgeInputFilename,
      convertConfig.edgeLineFilter,
      convertConfig.edgeSeparator,
      convertConfig.srcNodeColumn,
      convertConfig.dstNodeColumn,
      convertConfig.weightColumn,
      convertConfig.edgeAnalytics)
    
    infile_node.foreach { nodeFile =>
      g.readMetadata(nodeFile, node_filter, node_separator, node_id_column, node_metadata_column, node_analytics)
    }

    if (convertConfig.renumber)
      g = g.renumber()

    val edgeStream = new DataOutputStream(new FileOutputStream(convertConfig.edgeOutputFilename))
    val weightStream = convertConfig.weightOutputFilename.map(filename => new DataOutputStream(new FileOutputStream(filename)))
    val metadataStream = convertConfig.metaOutputFilename.map(filename => new DataOutputStream(new FileOutputStream(filename)))
    g.display_binary(edgeStream, weightStream, metadataStream)
    edgeStream.flush(); edgeStream.close()
    weightStream.foreach{s => s.flush(); s.close()}
    metadataStream.foreach{s => s.flush(); s.close()}
  }
}
