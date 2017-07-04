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
import software.uncharted.contrib.tiling.config.ConfigParser

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.util.Try

import scala.collection.JavaConversions._ //scalastyle:ignore

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
  *
  * Valid properties are:
  *
  *   - `edge.input`  - Edge input file.
  *   - `edge.output` - Edge output file.
  *   - `edge.filter` - Filter used to keep only edge lines from the edge input.
  *   - `edge.analytic` - Analytic to apply to edges.
  *   - `edge.separator`  - Separator used in the edge input file.
  *   - `edge.source-column`  - Column containing the source node in the edge input.
  *   - `edge.destination-column` - Column containing the destination node in the edge input.
  *   - `edge.weight-column`  - Column containing the edge weight in the edge input.
  *   - `node.input`  - Node input file.
  *   - `node.weight-output`  - Output file to use for the node weight data.
  *   - `node.filter` - Filter used to keep only node lines from the node input.
  *   - `node.separator`  - Separator used in the node input.
  *   - `node.analytic` - Analytic to apply to nodes.
  *   - `node.node-column`  - Node id column in the node input.
  *   - `node.meta-column`  - Meta data column in the node input.
  *   - `renumber`  - If true, will renumber nodes from 0 to n.
  *   - `node.meta-output`  - Node metadata output file.
  *
  *   Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *   convert {
  *   edge {
  *     input = "edges"
  *     output = "edges.bin"
  *     separator = "\t"
  *     source-column = 0
  *     destination-column = 1
  *   }
  *   node {
  *     input = "nodes"
  *     separator = "\t"
  *     node-column = 1
  *     meta-column = 0
  *     meta-output = metadata.bin
  *     analytic = "software.uncharted.graphing.analytics.BucketAnalytic:../config/grant-analytics.conf"
  *   }
  * }
  */
object ConvertConfigParser extends ConfigParser {

  val SectionKey = "convert"
  val EdgeInput = "edge.input"
  val EdgeOutput = "edge.output"
  val EdgeFilter = "edge.filter"
  val EdgeAnalytic = "edge.analytic"
  val EdgeSeparator = "edge.separator"
  val SrcNodeColumn = "edge.source-column"
  val DstNodeColumn = "edge.destination-column"
  val WeightColumn = "edge.weight-column"
  val NodeInput = "node.input"
  val WeightOutput = "node.weight-output"
  val NodeFilter = "node.filter"
  val NodeSeparator = "node.separator"
  val NodeAnalytic = "node.analytics-string"
  val NodeAnalyticSection = "node.analytics"
  val NodeColumn = "node.node-column"
  val MetaColumn = "node.meta-column"
  val Renumber = "renumber"
  val MetaOutput = "node.meta-output"

  private val AnalyticClass = "class"
  private val AnalyticConfig = "config"

  /**
    * Parse the convert configuration into the wrapper class.
 *
    * @param config Configuration values to use when converting.
    * @return The parsed configuration.
    */
  def parse(config: Config): Try[ConvertConfig] = {
    Try {
      val section = config.getConfig(SectionKey)

      // Parse objects needed for configuration.
      // Analytics should probably be setup to have a subsection and should be iterated over.
      var edgeAnalytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      if (section.hasPath(EdgeAnalytic)) {
        section.getString(EdgeAnalytic).split(",").foreach(ea => edgeAnalytics += CustomGraphAnalytic(ea, ""))
      }

      var nodeAnalytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()
      if (section.hasPath(NodeAnalytic)) {
        section.getString(NodeAnalytic).split(",").foreach(na => {
          val naSplit = na.split(":")
          nodeAnalytics += CustomGraphAnalytic(naSplit(0), if (naSplit.length > 1) naSplit(1) else "")
        })
      } else if (section.hasPath(NodeAnalyticSection)) {
        val configs = section.getConfigList(NodeAnalyticSection)
        nodeAnalytics = configs.map(c => {
          CustomGraphAnalytic(
            c.getString(AnalyticClass),
            if  (c.hasPath(AnalyticClass)) Some(c.getConfig(AnalyticConfig)) else None
          )
        })
      }

      ConvertConfig(
        section.getString(EdgeInput),
        getStringOption(section, EdgeFilter),
        section.getString(EdgeSeparator),
        edgeAnalytics,
        section.getInt(SrcNodeColumn),
        section.getInt(DstNodeColumn),
        getIntOption(section, WeightColumn),
        section.getString(EdgeOutput),
        getStringOption(section, NodeInput),
        getStringOption(section, NodeFilter),
        section.getString(NodeSeparator),
        nodeAnalytics,
        section.getInt(NodeColumn),
        section.getInt(MetaColumn),
        getBoolean(section, Renumber, false),
        getStringOption(section, WeightOutput),
        getStringOption(section, MetaOutput)
      )
    }
  }
}
