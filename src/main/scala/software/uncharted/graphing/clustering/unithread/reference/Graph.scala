/**
 * This code is copied and translated from https://sites.google.com/site/findcommunities
 *
 * This means it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
 * we can't distribute it without permission - though as a translation, with some optimization for readability in
 * scala, it may be a gray area.
 */
package software.uncharted.graphing.clustering.unithread.reference

import java.io.{DataInputStream, FileInputStream}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.{Graph => SparkGraph}

import scala.collection.mutable.Buffer

/**
 * based on graph_binary.h and graph_binary.cpp from Blondel et al
 *
 * @param degrees A list of the cummulative degree of each node, in order:
 *                deg(0) = degrees[0]
 *                deg(k) = degrees[k]=degrees[k-1]
 * @param links A list of the links to other nodes
 * @param weightsOpt An optional list of the weight of each link; if existing, it must be the same size as links
 */
class Graph (degrees: Array[Int], links: Array[Int], weightsOpt: Option[Array[Float]] = None) {
  val nb_nodes = degrees.size
  val nb_links = links.size
  val total_weight =
    (for (i <- 0 until nb_nodes) yield weighted_degree(i)).fold(0.0)(_ + _)



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

  def weighted_degree (node: Int): Double =
    weightsOpt.map(weights =>
      neighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
    ).getOrElse(nb_neighbors(node))

  def display: Unit =
    (0 until nb_nodes).foreach { node =>
      println(node+":"+neighbors(node).mkString(" "))
    }

  def display_reverse: Unit =
    (0 until nb_nodes).foreach{ node =>
      neighbors(node).foreach{ case (dst, weight) =>
        println(dst+" "+node+" "+weight)
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

    new Graph(cumulativeDegrees, links, weights)
  }


  def apply (filename: String, filename_w: Option[String], weighted: Boolean): Graph = {
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

    val weights:Option[Array[Float]] = if (weighted) {
      val finput_w = new DataInputStream(new FileInputStream(filename_w.get))
      val weightsInner = new Array[Float](nb_links)
      for (i <- 0 until nb_links) weightsInner(i) = finput_w.readFloat
      finput_w.close
      Some(weightsInner)
    } else {
      None
    }
    finput.close

    new Graph(degrees, links, weights)
  }
}
