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



import java.io.{DataInputStream, FileInputStream, PrintStream}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph => SparkGraph, VertexId}
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.salt.core.analytic.Aggregator

/**
  * Wrapper around a node (community) in the graph.
  * @param id Id of the node (community).
  * @param internalNodes Number of internal nodes for the community.
  * @param metaData Metadata associated with the node.
  * @param analyticData Analytic data associated with the node.
  * @param analytics Analytics to run when generating communities.
  */
case class NodeInfo (id: Long, internalNodes: Int, metaData: Option[String],
                     analyticData: Array[Any], analytics: Array[CustomGraphAnalytic[_]]) {
  var communityNode: Option[NodeInfo] = None

  private def finishValue[AIT] (rawValue: Any, analytic: CustomGraphAnalytic[AIT]) =
    analytic.aggregator.finish(rawValue.asInstanceOf[AIT])

  /**
    * Finalize the analytics values into a collection of strings.
    * @return The finalized analytics values.
    */
  def finishedAnalyticValues: Array[String] = {
    (analyticData zip analytics).map{case (data, analytic) =>
        finishValue(data, analytic)
    }
  }

  private def getCurrentValue[AIT] (rawValue: Any, analytic: CustomGraphAnalytic[AIT]) = rawValue.asInstanceOf[AIT]
  private def mergeCurrentValues[AIT] (left: Any, right: Any, analytic: CustomGraphAnalytic[AIT]): AIT = {
    val typedLeft = getCurrentValue(left, analytic)
    val typedRight = getCurrentValue(right, analytic)
    analytic.aggregator.merge(typedLeft, typedRight)
  }

  //scalastyle:off method.name
  /**
    * Combine two nodes together.
    * @param that The other node.
    * @return The new combined node.
    */
  def +(that: NodeInfo): NodeInfo = {
    val aggregatedAnalyticData = for (i <- analytics.indices) yield {
      val a = analytics(i)
      val left = if (i < this.analyticData.length) this.analyticData(i) else null //scalastyle:ignore
      val right = if (i < that.analyticData.length) that.analyticData(i) else null //scalastyle:ignore
      mergeCurrentValues(left, right, a)
    }
    NodeInfo(
      this.id,
      this.internalNodes + that.internalNodes,
      this.metaData,
      aggregatedAnalyticData.toArray,
      analytics
    )
  }
  //scalastyle:on method.name
}

/**
 * based on graph_binary.h and graph_binary.cpp from Blondel et al
 *
 * @param degrees A list of the cumulative degree of each node, in order:
 *                deg(0) = degrees[0]
 *                deg(k) = degrees[k]-degrees[k-1]
 * @param links A list of the links to other nodes
 * @param nodeInfos Extra information about each node
 * @param weightsOpt An optional list of the weight of each link; if existing, it must be the same size as links
 */
class Graph (degrees: Array[Int], links: Array[Int], nodeInfos: Array[NodeInfo], weightsOpt: Option[Array[Float]] = None) {
  val nb_nodes = degrees.length
  val nb_links = links.length
  // A place to cache node weights, so it doesn't have to be calculated multiple times.
  private val weights = new Array[Option[Double]](nb_nodes)
  val total_weight =
    (for (i <- 0 until nb_nodes) yield weightedDegree(i)).fold(0.0)(_ + _)


  def id (node: Int): Long = nodeInfos(node).id
  def internalSize (node: Int): Int = nodeInfos(node).internalNodes
  def metaData (node: Int): String = nodeInfos(node).metaData.getOrElse("")
  def nodeInfo (node: Int): NodeInfo = nodeInfos(node)

  /**
    * The number of neighbors.
    * @param node The node.
    * @return The number of neighbors for the node.
    */
  def nbNeighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)
    } else {
      degrees(node) - degrees(node - 1)
    }

  /**
    * Iterator for the node's neighbors.
    * @param node The node.
    * @return The new iterator.
    */
  def neighbors (node: Int): Iterator[(Int, Float)] =
    new NeighborIterator(node)

  /**
    * Get the weighted number of self loops.
    * @param node The node.
    * @return The sum of weights of all self loops on the node.
    */
  def nbSelfLoops (node: Int): Double =
    neighbors(node).filter(_._1 == node).map(_._2).fold(0.0f)(_ + _)

  /**
    * Get the weighted degree. Note that these values are cached internally.
    * @param node The node.
    * @return The weighted degree of the node.
    */
  def weightedDegree (node: Int): Double = {
    // Only calculated the degree of a node once
    if (null == weights(node) || weights(node).isEmpty) { //scalastyle:ignore
      weights(node) = Some(weightsOpt.map(weights =>
        neighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
      ).getOrElse(nbNeighbors(node))
      )
    }
    weights(node).get
  }

  /**
    * Output the graph's nodes.
    * @param out Output stream to output to.
    */
  def displayNodes (out: PrintStream): Unit = {
    (0 until nb_nodes).foreach { node =>
      out.println("node\t" + id(node) + "\t" + internalSize(node) + "\t" + weightedDegree(node) + "\t" + metaData(node)) //scalastyle:ignore
    }
  }

  /**
    * Output the graph's links.
    * @param out Output stream to output to.
    */
  def displayLinks (out: PrintStream): Unit = {
    (0 until nb_nodes).foreach { node =>
      neighbors(node).foreach { case (dst, weight) =>
        out.println("edge\t" + id(node) + "\t" + id(dst) + "\t" + weight.round) //scalastyle:ignore
      }
    }
  }

  /**
    * Iterator for a node's neighbors.
    * @param node The node.
    */
  class NeighborIterator (node: Int) extends Iterator[(Int, Float)] {
    var index= if (0 == node) 0 else degrees(node-1)
    val end = degrees(node)

    override def hasNext: Boolean = index < end

    override def next(): (Int, Float) = {
      val nextLink: Int = links(index)
      val nextWeight = weightsOpt.map(_(index)).getOrElse(1.0f)
      index = index + 1
      (nextLink, nextWeight)
    }
  }

  /**
    * Transform the graph into a parallelized spark graph.
    * @param sc Spark context to use.
    * @return The parallelized spark graph.
    */
  def toSpark(sc: SparkContext): SparkGraph[Int, Float] = {
    val nodes = (0 until nb_nodes).map(n => (n.toLong, n))
    var i = 0
    val edges = for (src <- 0 until nb_nodes; j <- 0 until degrees(src)) yield {
      val target = links(i)
      val weight = weightsOpt.map(_(i)).getOrElse(1.0f)
      i = i + 1

      new Edge(src, target, weight)
    }
    SparkGraph(sc.parallelize(nodes), sc.parallelize(edges))
  }
}

//scalastyle:off method.length
object Graph {
  def apply[VD, ED] (source: org.apache.spark.graphx.Graph[VD, ED],
                     getEdgeWeight: Option[ED => Float] = None,
                     extractMetadataValue: VD => String,
                     extractAnalyticValues: Option[VD => Seq[String]],
                     customGraphAnalytics: Array[CustomGraphAnalytic[_]]): Graph = {
    def getAnalyticValues (node: VD): Array[Any] = {
      val values = new Array[Any](customGraphAnalytics.length)
      val inputValues = extractAnalyticValues.map(_(node)).getOrElse(Seq[String]())
      def extractValue[T] (index: Int, analytic: CustomGraphAnalytic[T]): T = {
        val a: Aggregator[String, T, String] = analytic.aggregator
        val default = a.default()
        if (index < inputValues.length) {
          a.add(default, Some(inputValues(index)))
        } else {
          a.add(default, None)
        }
      }
      for (i <- customGraphAnalytics.indices) {
        values(i) = extractValue(i, customGraphAnalytics(i))
      }
      values
    }

    val nodes: Array[(VertexId, String, Array[Any])] =
      source.vertices.map(v => (v._1, extractMetadataValue(v._2), getAnalyticValues(v._2))).collect.sortBy(_._1)
    val edges = source.edges.collect.map(edge => (edge.srcId, edge.dstId, edge.attr))
    val minNode = nodes.map(_._1).min
    val maxNode = nodes.map(_._1).max
    val nb_nodes = (maxNode - minNode + 1).toInt

    // Note that, as in the original, a link between two nodes contributes its full weight (and degree) to both nodes,
    // whereas a self-link only contributes its weight to that one node once - hence seemingly being counted half as
    // much! (at least, that's what I read as going on)
    val cumulativeDegrees = new Array[Int](nb_nodes)
    var nb_links = 0
    for (i <- 0 until nb_nodes) {
      val node = minNode + i
      val degrees = edges.count(edge => node == edge._1 || node == edge._2)
      nb_links = nb_links + degrees
      cumulativeDegrees(i) = nb_links
    }

    val nodeInfos = new Array[NodeInfo](nb_nodes)
    for (i <- 0 until nb_nodes) {
      nodeInfos(i) = NodeInfo(nodes(i)._1, 1, Some(nodes(i)._2), nodes(i)._3, customGraphAnalytics)
    }

    val links = new Array[Int](nb_links)
    var linkNum = 0
    for (i <- 0 until nb_nodes) {
      val node = minNode + i
      val relevantEdges = edges.filter(edge => node == edge._1 || node == edge._2)
      val directedEdges = relevantEdges.map{edge =>
        if (node == edge._1) (edge._2 - minNode).toInt else (edge._1 - minNode).toInt
      }
      directedEdges.foreach { destination =>
        links(linkNum) = destination
        linkNum = linkNum + 1
      }
    }
    val weights: Option[Array[Float]] = getEdgeWeight.map { edgeWeightFcn =>
      val weightsInner = new Array[Float](nb_links)

      linkNum = 0
      for (i <- 0 until nb_nodes) {
        val node = minNode + i
        val edgeWeights = edges.filter(edge => node == edge._1 || node == edge._2).map(edge =>
          edgeWeightFcn(edge._3)
        )
        edgeWeights.foreach { edgeWeight =>
          weightsInner(linkNum) = edgeWeight
          linkNum = linkNum + 1
        }
      }
      weightsInner
    }

    new Graph(cumulativeDegrees, links, nodeInfos, weights)
  }


  /**
    * Create a graph instance from source files.
    * @param filename Edge data source file.
    * @param filename_w_opt Weight data source file.
    * @param filename_m_opt Metadata source file.
    * @param customAnalytics Analytics to run on the graph nodes.
    * @return Graph instance of the source file data.
    */
  def apply (filename: String, filename_w_opt: Option[String], filename_m_opt: Option[String],
             customAnalytics: Array[CustomGraphAnalytic[_]]): Graph = {
    val finput = new DataInputStream(new FileInputStream(filename))
    val finput_w_opt = filename_w_opt.map(filename_w => new DataInputStream(new FileInputStream(filename_w)))
    val finput_m_opt = filename_m_opt.map(filename_m => new DataInputStream(new FileInputStream(filename_m)))

    val result = apply(finput, finput_w_opt, finput_m_opt, customAnalytics)

    finput.close()
    finput_w_opt.foreach(_.close())
    finput_m_opt.foreach(_.close())

    result
  }

  /**
    * Create a graph from source streams.
    * @param edgeDataStream Edge data source stream.
    * @param weightDataStreamOpt Weight data source stream.
    * @param metadataInputStreamOpt Metadata source stream.
    * @param customAnalytics Analytics to run on the graph nodes.
    * @return Graph instance of the source streams.
    */
  def apply (edgeDataStream: DataInputStream,
             weightDataStreamOpt: Option[DataInputStream],
             metadataInputStreamOpt: Option[DataInputStream],
             customAnalytics: Array[CustomGraphAnalytic[_]]): Graph = {
    val nb_nodes = edgeDataStream.readInt

    // Read cumulative degree sequence (long per node)
    // cum_degree[0] = degree(0), cum_degree[1] = degree(0)+degree(1), etc.
    val degrees = new Array[Int](nb_nodes)
    for (i <- 0 until nb_nodes) degrees(i) = edgeDataStream.readLong.toInt

    // Read links (int per node)
    val nb_links = degrees(nb_nodes-1)
    val links = new Array[Int](nb_links)
    for (i <- 0 until nb_links) links(i) = edgeDataStream.readInt



    val weights:Option[Array[Float]] =
      weightDataStreamOpt.map { weightDataStream =>
        val weightsInner = new Array[Float](nb_links)
        for (i <- 0 until nb_links) weightsInner(i) = weightDataStream.readFloat()
        weightsInner
      }

    val nodeInfos = new Array[NodeInfo](nb_nodes)
    if (metadataInputStreamOpt.isDefined) {
      def extractAnalyticValue[T] (aggregator: Aggregator[String, T, String], value: String) =
        aggregator.add(aggregator.default(), Some(value))

      metadataInputStreamOpt.foreach { metadataInputStream =>
        for (i <- 0 until nb_nodes) {
          val md = metadataInputStream.readUTF()
          val ad = new Array[Any](metadataInputStream.readInt())
          for (i <- ad.indices) {
            ad(i) = extractAnalyticValue(customAnalytics(i).aggregator, metadataInputStream.readUTF())
          }
          nodeInfos(i) = NodeInfo(i, 1, Some(md), ad, customAnalytics)
        }
      }
    } else {
      for (i <- 0 until nb_nodes)
        nodeInfos(i) = NodeInfo(i, 1, None, Array[Any](), customAnalytics)
    }

    new Graph(degrees, links, nodeInfos, weights)
  }
}
//scalastyle:on method.length
