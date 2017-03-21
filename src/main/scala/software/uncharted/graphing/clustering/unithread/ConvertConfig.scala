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
package software.uncharted.graphing.clustering.unithread

import com.typesafe.config.Config
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.util.Try

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
  val WEIGHT_OUTPUT = "weight.output"
  val NODE_FILTER = "node.filter"
  val NODE_SEPARATOR = "node.separator"
  val NODE_ANALYTIC = "node.analytic"
  val NODE_COLUMN = "node.node-column"
  val META_COLUMN = "node.meta-column"
  val RENUMBER = "renumber"
  val META_OUTPUT = "node.meta-output"

  def parse(config: Config): Try[ConvertConfig] = {
    Try {
      val section = config.getConfig(SECTION_KEY)

      // Parse objects needed for configuration.
      var edgeAnalytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      section.getString(EDGE_ANALYTIC).split(",").foreach(ea => edgeAnalytics += CustomGraphAnalytic(ea, ""))

      var nodeAnalytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      section.getString(NODE_ANALYTIC).split(",").foreach(na => {
        val naSplit = na.split(":")
        nodeAnalytics += CustomGraphAnalytic(naSplit(0), if (naSplit.length > 1) naSplit(1) else "")
      })

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
        section.getBoolean(RENUMBER),
        getStringOption(section, WEIGHT_OUTPUT),
        getStringOption(section, META_OUTPUT)
      )
    }
  }
}
