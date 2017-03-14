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

import scala.collection.mutable.{Buffer => MutableBuffer}
import software.uncharted.graphing.analytics.CustomGraphAnalytic



object Convert {
  var infile_edge: Option[String] = None
  var edge_filter: Option[String] = None
  var edge_separator = "[ \t]+"
  var edge_source_column = 0
  var edge_destination_column = 1
  var edge_weight_column: Option[Int] = None
  var edge_analytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()

  var infile_node: Option[String] = None
  var node_filter: Option[String] = None
  var node_separator = "[ \t]+"
  var node_id_column = 0
  var node_metadata_column = 1
  var node_analytics: MutableBuffer[CustomGraphAnalytic[_]] = MutableBuffer()

  var outfile: Option[String] = None
  var outfile_weight: Option[String] = None
  var outfile_metadata: Option[String] = None
  var do_renumber = false

  def usage(prog_name: String, more: String): Unit = {
    println(more)
    println("usage: " + prog_name + " -i input_file -o outfile [-r] [-w outfile_weight]")
    println
    println("read the graph and convert it to binary format.")
    println("Edge input parameters:")
    println("-ie filename\tinput edge file name")
    println("-fe string\tfilter lines in the edge file to ones that contain the specified string.")
    println("-ce string\tseparator character in edge file (defaults to \"[ \t]+\")")
    println("-ae customAnalytic\tThe fully qualified name of a class describing a custom analytic to run on the edge data.  Multiple instances allowed, and performed in order.")
    println("-s column\tsource node id column")
    println("-d column\tdestination node id column")
    println("-w column\tweight column")
    println("Node input parameters (optional, only use is for metadata)")
    println("-in filename\tinput node file name")
    println("-fn string\tfilter lines in the node file for those that contain the specified string.")
    println("-cn string\tseparator string in node file (defaults to \"[ \t]+\")")
    println("-an customAnalytic\tThe fully qualified name of a class describing a custom analytic to run on the node data.  Multiple instances allowed, and performed in order.")
    println("-n column\tnode id column")
    println("-m column\tmeta-data column")
    println("-r\tnodes are renumbered from 0 to nb_nodes-1 (the order is kept).")
    println("-oe filename\tThe file name to which to write the output edge file")
    println("-ow filename\tread the graph as a weighted one and writes the weights in a separate file.")
    println("-om filename\tThe file name to which to write the output metadata file")
    println("-h\tshow this usage message.")

    System.exit(0)
  }

  def parse_args(args: Array[String]): Unit = {
    var i = 0
    while (i < args.length) {
      if (args(i).startsWith("-")) {
        args(i).substring(1).toLowerCase match {
          // Edge parameters
          case "ie" =>
            i = i + 1
            infile_edge = Some(args(i))

          case "fe" =>
            i = i + 1
            edge_filter = Some(args(i))

          case "ce" =>
            i = i + 1
            edge_separator = args(i)

          case "ae" =>
            i = i + 1
            edge_analytics += CustomGraphAnalytic(args(i), "")

          case "s" =>
            i = i + 1
            edge_source_column = args(i).toInt

          case "d" =>
            i = i + 1
            edge_destination_column = args(i).toInt

          case "w" =>
            i = i + 1
            edge_weight_column = Some(args(i).toInt)


          // Node parameters
          case "in" =>
            i = i + 1
            infile_node = Some(args(i))

          case "fn" =>
            i = i + 1
            node_filter = Some(args(i))

          case "cn" =>
            i = i + 1
            node_separator = args(i)

          case "an" =>
            i = i + 1
            node_analytics += CustomGraphAnalytic(args(i), "")

          case "anc" =>
            i = i + 1
            val analytic = args(i)
            i = i + 1
            node_analytics += CustomGraphAnalytic(analytic, args(i))

          case "n" =>
            i = i + 1
            node_id_column = args(i).toInt

          case "m" =>
            i = i + 1
            node_metadata_column = args(i).toInt



          // Output parameters
          case "oe" =>
            i = i + 1
            outfile = Some(args(i))

          case "ow" =>
            i = i + 1
            outfile_weight = Some(args(i))

          case "om" =>
            i = i + 1
            outfile_metadata = Some(args(i))

          case "r" => do_renumber = true
          case _ => usage("convert", "Unknown option: " + args(i))
        }
      }
      i = i + 1
    }
  }

  def main(args: Array[String]): Unit = {
    parse_args(args)
//    val edgeReader = new BufferedReader(new InputStreamReader(new FileInputStream(infile_edge.get)))
//    var g = GraphEdges(edgeReader, edge_filter, edge_separator, edge_source_column, edge_destination_column, edge_weight_column)
//    edgeReader.close()
    var g = GraphEdges(infile_edge.get, edge_filter, edge_separator, edge_source_column, edge_destination_column, edge_weight_column, edge_analytics)

    infile_node.foreach { nodeFile =>
      g.readMetadata(nodeFile, node_filter, node_separator, node_id_column, node_metadata_column, node_analytics)
    }

    if (do_renumber)
      g = g.renumber()

    val edgeStream = new DataOutputStream(new FileOutputStream(outfile.get))
    val weightStream = outfile_weight.map(filename => new DataOutputStream(new FileOutputStream(filename)))
    val metadataStream = outfile_metadata.map(filename => new DataOutputStream(new FileOutputStream(filename)))
    g.display_binary(edgeStream, weightStream, metadataStream)
    edgeStream.flush(); edgeStream.close()
    weightStream.foreach{s => s.flush(); s.close()}
    metadataStream.foreach{s => s.flush(); s.close()}
  }
}
