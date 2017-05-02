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

import com.typesafe.config.Config
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.util.Try

/**
  * Wrapper for all the convert parameters needed.
  * @param edgeInputFilename Filename of the edge data.
  * @param edgeLineFilter String to use to filter for edge data in the edge input file.
  * @param edgeSeparator Field separator for the edge data.
  * @param edgeAnalytics Aggregation analytics to use on edge data.
  * @param srcNodeColumn 0 based column of the source node.
  * @param dstNodeColumn 0 based column of the destination node.
  * @param weightColumn 0 based column of the weight.
  * @param edgeOutputFilename Filename to use for the edge output.
  * @param nodeInputFilename Filename of the ndoe data.
  * @param nodeLineFilter String to use to filter for node data in the node input file.
  * @param nodeSeparator Field separator for the node data.
  * @param nodeAnalytics Aggregation analytics to use on node data.
  * @param nodeColumn 0 based node id column.
  * @param metaColumn 0 based metadata column.
  * @param renumber Flag to renumber node ids from 0 to n.
  * @param weightOutputFilename Output filename of the weight data.
  * @param metaOutputFilename Output filename of the metadata.
  */
case class ConvertConfig(edgeInputFilename: String,
                         edgeLineFilter: Option[String],
                         edgeSeparator: String,
                         edgeAnalytics: Seq[CustomGraphAnalytic[_]],
                         srcNodeColumn: Int,
                         dstNodeColumn: Int,
                         weightColumn: Option[Int],
                         edgeOutputFilename: String,
                         nodeInputFilename: Option[String],
                         nodeLineFilter: Option[String],
                         nodeSeparator: String,
                         nodeAnalytics: Seq[CustomGraphAnalytic[_]],
                         nodeColumn: Int,
                         metaColumn: Int,
                         renumber: Boolean,
                         weightOutputFilename: Option[String],
                         metaOutputFilename: Option[String])

/**
  * Parser of the convert configuration.
  */
object ConvertConfigParser extends ConfigParser {

  val SECTION_KEY = "convert"
  val EDGE_INPUT = "edge.input"
  val EDGE_OUTPUT = "edge.output"
  val EDGE_FILTER = "edge.filter"
  val EDGE_ANALYTIC = "edge.analytic"
  val EDGE_SEPARATOR = "edge.separator"
  val SRC_NODE_COLUMN = "edge.source-column"
  val DST_NODE_COLUMN = "edge.destination-column"
  val WEIGHT_COLUMN = "edge.weight-column"
  val NODE_INPUT = "node.input"
  val WEIGHT_OUTPUT = "node.weight-output"
  val NODE_FILTER = "node.filter"
  val NODE_SEPARATOR = "node.separator"
  val NODE_ANALYTIC = "node.analytic"
  val NODE_COLUMN = "node.node-column"
  val META_COLUMN = "node.meta-column"
  val RENUMBER = "renumber"
  val META_OUTPUT = "node.meta-output"

  /**
    * Parse the convert configuration into the wrapper class.
    * @param config Configuration values to use when converting.
    * @return The parsed configuration.
    */
  def parse(config: Config): Try[ConvertConfig] = {
    Try {
      val section = config.getConfig(SECTION_KEY)

      // Parse objects needed for configuration.
      // Analytics should probably be setup to have a subsection and should be iterated over.
      var edgeAnalytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      if (section.hasPath(EDGE_ANALYTIC)) {
        section.getString(EDGE_ANALYTIC).split(",").foreach(ea => edgeAnalytics += CustomGraphAnalytic(ea, ""))
      }

      var nodeAnalytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      if (section.hasPath(NODE_ANALYTIC)) {
        section.getString(NODE_ANALYTIC).split(",").foreach(na => {
          val naSplit = na.split(":")
          nodeAnalytics += CustomGraphAnalytic(naSplit(0), if (naSplit.length > 1) naSplit(1) else "")
        })
      }

      ConvertConfig(
        section.getString(EDGE_INPUT),
        getStringOption(section, EDGE_FILTER),
        section.getString(EDGE_SEPARATOR),
        edgeAnalytics,
        section.getInt(SRC_NODE_COLUMN),
        section.getInt(DST_NODE_COLUMN),
        getIntOption(section, WEIGHT_COLUMN),
        section.getString(EDGE_OUTPUT),
        getStringOption(section, NODE_INPUT),
        getStringOption(section, NODE_FILTER),
        section.getString(NODE_SEPARATOR),
        nodeAnalytics,
        section.getInt(NODE_COLUMN),
        section.getInt(META_COLUMN),
        getBoolean(section, RENUMBER, false),
        getStringOption(section, WEIGHT_OUTPUT),
        getStringOption(section, META_OUTPUT)
      )
    }
  }
}
