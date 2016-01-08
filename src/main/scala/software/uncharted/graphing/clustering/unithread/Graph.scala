/**
 * This code is copied and translated from https://sites.google.com/site/findcommunities
 *
 * This means it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
 * we can't distribute it without permission - though as a translation, with some optimization for readability in
 * scala, it may be a gray area.
 */
package software.uncharted.graphing.clustering.unithread

import java.io.{DataInputStream, FileInputStream, PrintStream}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph => SparkGraph, VertexId}

case class NodeInfo (id: Long, internalNodes: Int, metaData: Option[String]) {
  def + (that: NodeInfo): NodeInfo =
  NodeInfo(this.id, this.internalNodes + that.internalNodes, this.metaData)
}

/**
 * based on graph_binary.h and graph_binary.cpp from Blondel et al
 *
 * @param degrees A list of the cummulative degree of each node, in order:
 *                deg(0) = degrees[0]
 *                deg(k) = degrees[k]=degrees[k-1]
 * @param links A list of the links to other nodes
 * @param nodeInfos Extra information about each node
 * @param weightsOpt An optional list of the weight of each link; if existing, it must be the same size as links
 */
class Graph (degrees: Array[Int], links: Array[Int], nodeInfos: Array[NodeInfo], weightsOpt: Option[Array[Float]] = None) {
  val nb_nodes = degrees.size
  val nb_links = links.size
  // A place to cache node weights, so it doesn't have to be calculated multiple times.
  private val weights = new Array[Option[Double]](nb_nodes)
  val total_weight =
    (for (i <- 0 until nb_nodes) yield weighted_degree(i)).fold(0.0)(_ + _)


  def id (node: Int): Long = nodeInfos(node).id
  def internalSize (node: Int): Int = nodeInfos(node).internalNodes
  def metaData (node: Int): String = nodeInfos(node).metaData.getOrElse("")
  def nodeInfo (node: Int): NodeInfo = nodeInfos(node)

  def nb_neighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)
    } else {
      degrees(node) - degrees(node - 1)
    }

  def neighbors (node: Int): Iterator[(Int, Float)] =
    new NeighborIterator(node)

  def nb_selfloops (node: Int): Double =
    neighbors(node).filter(_._1 == node).map(_._2).fold(0.0f)(_ + _)

  def weighted_degree (node: Int): Double = {
    // Only calculated the degree of a node once
    if (null == weights(node) || !weights(node).isDefined) {
      weights(node) = Some(weightsOpt.map(weights =>
        neighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
      ).getOrElse(nb_neighbors(node))
      )
    }
    weights(node).get
  }

  def display_nodes (out: PrintStream): Unit = {
    (0 until nb_nodes).foreach { node =>
      out.println("node\t"+id(node)+"\t"+internalSize(node)+"\t"+weighted_degree(node)+"\t"+metaData(node))
    }
  }
  def display_links (out: PrintStream): Unit = {
    (0 until nb_nodes).foreach { node =>
      neighbors(node).foreach { case (dst, weight) =>
        out.println("edge\t"+id(node)+"\t"+id(dst)+"\t"+weight.round.toInt)
      }
    }
  }

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

object Graph {
  def apply[VD, ED] (source: org.apache.spark.graphx.Graph[VD, ED], getEdgeWeight: Option[ED => Float] = None): Graph = {
    val nodes: Array[(VertexId, VD)] = source.vertices.collect.sortBy(_._1)
    val edges = source.edges.collect.map(edge => (edge.srcId, edge.dstId, edge.attr))
    val minNode = nodes.map(_._1).reduce(_ min _)
    val maxNode = nodes.map(_._1).reduce(_ max _)
    val nb_nodes = (maxNode - minNode + 1).toInt

    // Note that, as in the original, a link betwee two nodes contributes its full weight (and degree) to both nodes,
    // whereas a self-link only contributes its weight to that one node once - hence seemingly being counted half as
    // much! (at least, that's what I read as going on)
    val cumulativeDegrees = new Array[Int](nb_nodes)
    var nb_links = 0
    for (i <- 0 until nb_nodes) {
      val node = minNode + i
      val degrees = edges.filter(edge => node == edge._1 || node == edge._2).size
      nb_links = nb_links + degrees
      cumulativeDegrees(i) = nb_links
    }

    val nodeInfos = new Array[NodeInfo](nb_nodes)
    for (i <- 0 until nb_nodes) {
      nodeInfos(i) = NodeInfo(nodes(i)._1, 1, Some(nodes(i)._2.toString))
    }

    val links = new Array[Int](nb_links)
    var linkNum = 0
    for (i <- 0 until nb_nodes) {
      val node = minNode + i
      val relevantEdges = edges.filter(edge => node == edge._1 || node == edge._2)
      val directedEdges = relevantEdges.map{edge =>
        if (node == edge._1) (edge._2 - minNode).toInt
        else (edge._1 - minNode).toInt
      }
      directedEdges.map { destination =>
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
        edgeWeights.map { edgeWeight =>
          weightsInner(linkNum) = edgeWeight
          linkNum = linkNum + 1
        }
      }
      weightsInner
    }

    new Graph(cumulativeDegrees, links, nodeInfos, weights)
  }


  def apply (filename: String, filename_w: Option[String], filename_m: Option[String]): Graph = {
    val finput = new DataInputStream(new FileInputStream(filename))
    val nb_nodes = finput.readInt

    // Read cumulative degree sequence (long per node)
    // cum_degree[0] = degree(0), cum_degree[1] = degree(0)+degree(1), etc.
    val degrees = new Array[Int](nb_nodes)
    for (i <- 0 until nb_nodes) degrees(i) = finput.readLong.toInt

    // Read links (int per node)
    val nb_links = degrees(nb_nodes-1).toInt
    val links = new Array[Int](nb_links)
    for (i <- 0 until nb_links) links(i) = finput.readInt

    val weights:Option[Array[Float]] =
      filename_w.map { file =>
      val finput_w = new DataInputStream(new FileInputStream(file))
      val weightsInner = new Array[Float](nb_links)
      for (i <- 0 until nb_links) weightsInner(i) = finput_w.readFloat
      finput_w.close
      weightsInner
    }
    finput.close

    val nodeInfos = new Array[NodeInfo](nb_nodes)
    if (filename_m.isDefined) {
      filename_m.foreach { file =>
        val finput_m = new DataInputStream(new FileInputStream(file))
        for (i <- 0 until nb_nodes)
          nodeInfos(i) = NodeInfo(i, 1, Some(finput_m.readUTF()))
      }
    } else {
      for (i <- 0 until nb_links) nodeInfos(i) = NodeInfo(i, 1, None)
    }

    new Graph(degrees, links, nodeInfos, weights)
  }
}