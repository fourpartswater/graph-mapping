/**
 * This code is copied and translated from https://sites.google.com/site/findcommunities
 *
 * This means it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
 * we can't distribute it without permission - though as a translation, with some optimization for readability in
 * scala, it may be a gray area.
 */
package software.uncharted.graphing.clustering.reference

import org.apache.spark.graphx.VertexId

/**
 * based on graph_binary.h and graph_binary.cpp from Blondel et al
 *
 * @param degrees A list of the cummulative degree of each node, in order:
 *                deg(0) = degrees[0]
 *                deg(k) = degrees[k]=degrees[k-1]
 * @param links A list of the links to other nodes
 * @param weightsOpt An optional list of the weight of each link; if existing, it must be the same size as links
 */
class Graph (degrees: Seq[Int], links: Seq[Int], weightsOpt: Option[Seq[Double]] = None) {
  val nb_nodes = degrees.size
  val nb_links = links.size
  val total_weight =
    (for (i <- 0 until nb_nodes) yield weighted_degree(i)).fold(0.0)(_ + _)

  def nb_neighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)
    } else {
      val n1  = degrees(node)
      val n2 = degrees(node-1)
      degrees(node) - degrees(node - 1)
    }

  def neighbors (node: Int): Iterator[(Int, Double)] =
    new NeighborIterator(node)

  def nb_selfloops (node: Int): Double =
    neighbors(node).filter(_._1 == node).map(_._2).fold(0.0)(_ + _)

  def weighted_degree (node: Int): Double =
    weightsOpt.map(weights =>
      neighbors(node).map(_._2).fold(0.0)(_ + _)
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

  class NeighborIterator (node: Int) extends Iterator[(Int, Double)] {
    var index= if (0 == node) 0 else degrees(node-1)
    val end = degrees(node)

    override def hasNext: Boolean = index < end

    override def next(): (Int, Double) = {
      val nextLink: Int = links(index)
      val nextWeight = weightsOpt.map(_(index)).getOrElse(1.0)
      index = index + 1
      (nextLink, nextWeight)
    }
  }
}
object Graph {
  def apply[VD, ED] (source: org.apache.spark.graphx.Graph[VD, ED], getEdgeWeight: Option[ED => Double] = None): Graph = {
    val nodes: Array[(VertexId, VD)] = source.vertices.collect.sortBy(_._1)
    val edges = source.edges.collect.map(edge => (edge.srcId, edge.dstId, edge.attr))
    val minNode = nodes.map(_._1).reduce(_ min _)
    val maxNode = nodes.map(_._1).reduce(_ max _)
    val nb_nodes = (maxNode - minNode + 1).toInt
    // Note that, as in the original, a link betwee two nodes contributes its full weight (and degree) to both nodes,
    // whereas a self-link only contributes its weight to that one node once - hence seemingly being counted half as
    // much! (at least, that's what I read as going on)
    val degrees: Seq[Int] = (0 until nb_nodes).map { node =>
      val nodeL = minNode + node
      edges.filter(edge => nodeL == edge._1 || nodeL == edge._2).size
    }
    val cumulativeDegrees = (0 until degrees.size).map(n =>
      degrees.take(n+1).fold(0)(_ + _)
    )
    val links: Seq[Int] = (0 until nb_nodes).flatMap { node =>
      val nodeL = minNode + node
      val relevantEdges = edges.filter(edge => nodeL == edge._1 || nodeL == edge._2)
      val directedEdges = relevantEdges.map{edge =>
        if (nodeL == edge._1) (edge._2 - minNode).toInt
        else (edge._1 - minNode).toInt
      }
      directedEdges
    }
    val weights: Option[Seq[Double]] = getEdgeWeight.map(edgeWeightFcn =>
      (0 until nb_nodes).flatMap { node =>
        val nodeL = minNode + node
        edges.filter(edge => nodeL == edge._1 || nodeL == edge._2).map(edge =>
          edgeWeightFcn(edge._3)
        )
      }
    )

    new Graph(cumulativeDegrees, links, weights)
  }
}
