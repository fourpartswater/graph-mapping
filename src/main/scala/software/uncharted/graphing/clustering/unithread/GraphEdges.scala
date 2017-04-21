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
import java.util.Date

import software.uncharted.graphing.analytics.CustomGraphAnalytic

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.reflect.ClassTag


/**
  * An edge-base representation of a graph
  * @param links A list, by node, of the links in the graph.  Entry n describes node n, and is a list of
  *              (destination node, edge weight, link analytics values) for each edge.
  *              Link analytics are currently ignored.
  */
class GraphEdges (val links: Array[_ <: Seq[(Int, Float, Seq[String])]]) {
  /**
    * A list of descriptions of nodes
    * Entry n describes node n, and contains the metadata for that node, followed by a list of node analytic values
    * for that node
    */
  var metaData: Option[Array[(String, Seq[String])]] = None

  def readMetadata (metadataInput: BufferedReader, md_filter: Option[String], separator: String,
                    id_column: Int, md_column: Int, analytics: Seq[CustomGraphAnalytic[_]]): Unit = {
    metaData = Some(new Array[(String, Seq[String])](links.length))
    val analyticColumns = CustomGraphAnalytic.determineColumns(analytics)
    metaData.foreach{data =>
      var line = metadataInput.readLine()
      while (null != line) {
        if (!line.trim.isEmpty) {
          val fields = line.split(separator)
          if (md_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
            val nodeId = fields(id_column).toInt
            if (fields.size <= md_column) println("Too short")
            val md = fields(md_column)
            val analyticValues = analyticColumns.map { c =>
              if (fields.size <= c) ""
              else fields(c).trim
            }
            data(nodeId) = (md, analyticValues)
          }
        }

        line = metadataInput.readLine()
      }
    }
  }

  def renumber (weighted: Boolean): GraphEdges = {
    val linked = new Array[Boolean](links.length)
    val renum = new Array[Int](links.length)
    for (i <- links.indices) {
      linked(i) = false
      renum(i) = -1
    }

    for (i <- links.indices) {
      linked(i) = true
      for (j <- links(i).indices) {
        linked(links(i)(j)._1) = true
      }
    }

    var nb = 0
    for (i <- links.indices) {
      if (linked(i)) {
        renum(i) = nb
        nb = nb + 1
      }
    }

    val newLinks = new Array[MutableBuffer[(Int, Float, Seq[String])]](nb)
    for (i <- 0 until nb) newLinks(i) = MutableBuffer[(Int, Float, Seq[String])]()
    for (i <- links.indices) {
      if (linked(i)) {
        val ir = renum(i)
        val nll = newLinks(ir)
        for (j <- links(i).indices) {
          val lle = links(i)(j)
          nll.append((renum(lle._1), lle._2, lle._3))
        }
      }
    }

    val newGE = new GraphEdges(newLinks)
    metaData.foreach{md =>
      val newMetaData = new Array[(String, Seq[String])](nb)
      for (i <- links.indices)
        if (linked(i))
          newMetaData(renum(i)) = md(i)
      newGE.metaData = Some(newMetaData)
    }
    newGE
  }

  def display (weighted: Boolean): Unit = {
    for (i <- links.indices; j <- links(i).indices) {
      val (dest, weight, analyticValues) = links(i)(j)
      if (weighted) {
        println(i + " " + dest + " " + weight + analyticValues.mkString(" ", " ", ""))
      } else {
        println(i + " " + dest + analyticValues.mkString(" ", " ", ""))
      }
    }
  }

  def display_binary (edgeStream: DataOutputStream,
                      weightStream: Option[DataOutputStream],
                      metadataStream: Option[DataOutputStream]): Unit = {
    // output number of nodes
    val s = links.length
    edgeStream.writeInt(s)

    // output cumulative degree sequence
    var tot = 0L
    for (i <- 0 until s) {
      val degree: Int = links(i).size
      tot = tot + degree
      edgeStream.writeLong(tot)
    }

    // output links
    for (i <- 0 until s; j <- links(i).indices) {
      val dest = links(i)(j)._1
      edgeStream.writeInt(dest)
    }

    // Output weights to a separate file
    weightStream.foreach{stream =>
      for (i <- 0 until s; j <- links(i).indices) {
        val weight = links(i)(j)._2
        stream.writeFloat(weight)
      }
    }

    // Output metadata to yet another separate file
    metadataStream.foreach{stream =>
      metaData.foreach { data =>
        for (i <- 0 until s) {
          if (null == data(i)) {
            stream.writeUTF("")
            stream.writeInt(0)
          } else {
            val di = data(i)
            stream.writeUTF(di._1)
            stream.writeInt(di._2.length)
            di._2.foreach(dias => stream.writeUTF(dias))
          }
        }
      }
    }
  }

  /**
    * Convert the edge-based representation to a proper graph representation for in-line graph clustering
    * @param customAnalytics The analytics to apply to the graph nodes
    * @return A proper graph representing the same data
    */
  def toGraph (customAnalytics: Array[CustomGraphAnalytic[_]]): Graph = {
    // number of nodes
    val numNodes:         Int                  = links.length
    val degrees:          Array[Int]           = new Array[Int](numNodes)
    val nodes:            Array[NodeInfo]      = new Array(numNodes)

    // figure out how many edges there are total (and record the running total for each node)
    var totalEdges = 0
    var weighted = false
    for (i <- 0 until numNodes) {
      totalEdges = totalEdges + links(i).size
      degrees(i) = totalEdges
      weighted = weighted || links(i).exists(_._2 != 1.0f)
    }

    // Allocate destination and weight arrays
    val edgeDestinations: Array[Int]           = new Array(totalEdges)
    val edgeWeights:      Option[Array[Float]] =
      if (weighted) {
        Some(new Array[Float](totalEdges))
      } else {
        None
      }

    // Store nodes and weights
    var edgeIndex = 0
    for (i <- 0 until numNodes; j <- links(i).indices) {
      edgeDestinations(edgeIndex) = links(i)(j)._1
      edgeWeights.foreach(ew => ew(edgeIndex) = links(i)(j)._2)
      edgeIndex = edgeIndex + 1
    }


    for (i <- 0 until numNodes) {
      nodes(i) =
        metaData.map(_ (i)).map { case (metaDatum, analyticData) =>
          NodeInfo(i, 1, Some(metaDatum), analyticData.toArray, customAnalytics)
        }.getOrElse {
          NodeInfo(i, 1, None, Array[Any](), customAnalytics)
        }
    }

    new Graph(degrees, edgeDestinations, nodes, edgeWeights)
  }
}


object GraphEdges {
  def apply (edgeInputFile: String,
             edge_filter: Option[String],
             edge_separator: String,
             source_column: Int,
             destination_column: Int,
             weight_column: Option[Int],
             analytics: Seq[CustomGraphAnalytic[_]]): GraphEdges = {
    // First read through the file once, counting edges
    println("Counting nodes in graph")
    val countReader = new BufferedReader(new InputStreamReader(new FileInputStream(edgeInputFile)))
    var maxNode = 0
    var line = countReader.readLine()
    var n = 0
    while (null != line) {
      val fields = line.split(edge_separator)
      if (edge_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
        val source = fields(source_column).toInt
        val destination = fields(destination_column).toInt
        maxNode = maxNode max source max destination
      }

      line = countReader.readLine()
      n += 1
      if (0 == (n % 100000))
        println("Counted " + n + " ("+new Date()+")")
    }
    countReader.close()
    println("Reading graph with "+(maxNode+1)+" nodes")

    // Now actually read the graph
    val graphReader = new BufferedReader(new InputStreamReader(new FileInputStream(edgeInputFile)))
    val result = apply(graphReader, edge_filter, edge_separator, source_column, destination_column, weight_column, Some(maxNode+1))
    graphReader.close()
    result
  }

  def apply (edge_input: BufferedReader,
             edge_filter: Option[String],
             edge_separator: String,
             source_column: Int,
             destination_column: Int,
             weight_column: Option[Int],
             initialSize: Option[Int] = None,
             analytics: Seq[CustomGraphAnalytic[_]] = Seq()): GraphEdges = {
    val edges = new GrowableArray[MutableBuffer[(Int, Float, Seq[String])]](initialSize.getOrElse(0), () => MutableBuffer[(Int, Float, Seq[String])]())
    var line = edge_input.readLine()
    var n = 0
    val analyticColumns = CustomGraphAnalytic.determineColumns(analytics)
    while (null != line) {
      val fields = line.split(edge_separator)
      if (edge_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
        val source = fields(source_column).toInt
        val destination = fields(destination_column).toInt
        val weight = weight_column.map(c => fields(c).toFloat)
        val analyticValues = analyticColumns.map(c => fields(c))

        edges(source).append((destination, weight.getOrElse(1.0f), analyticValues))
        if (source != destination)
          edges(destination).append((source, weight.getOrElse(1.0f), analyticValues))
      }

      line = edge_input.readLine()
      n += 1
      if (0 == (n % 100000))
        println("Read " + n + " ("+new Date()+")")
    }
    new GraphEdges(edges.data)
  }
}

class GrowableArray[T: ClassTag](var size: Int = 0, initialize: () => T) {
  var data = {
    val initData = new Array[T](size)
    for (i <- 0 until size) initData(i) = initialize()
    initData
  }
  private def growTo (newSize: Int): Unit = {
    if (data.length < newSize) {
      println("Growing from "+data.length+" to "+newSize)
      val newData = new Array[T](newSize)
      for (i <- data.indices) newData(i) = data(i)
      for (i <- data.length until newSize) newData(i) = initialize()
      size = newSize
      data = newData
    }
  }
  def apply (n: Int): T = {
    growTo(n+1)
    data(n)
  }
  def update (n: Int, value: T) = {
    data(n) = value
  }
}
