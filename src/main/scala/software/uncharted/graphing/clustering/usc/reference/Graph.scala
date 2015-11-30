package software.uncharted.graphing.clustering.usc.reference

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}



/**
 * Copied and translated to scala from
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/Graph.java
 *
 * Some small changes:
 *    * We assume all graphs are weighted.  Just use weights of 1.0f if you don't have any inherently.
 *    * because of that, we can store links and weights in the same structure.
 */
class Graph (val degrees: Seq[Int], val links: Seq[(Int, Float)], val remoteLinks: Option[Seq[RemoteMap]]) extends Serializable {
  val nb_nodes: Int = degrees.size
  var nb_links: Long = links.size
  var formerlyRemoteLinks: Option[Map[Int, Seq[(Int, Float)]]] = None
  var total_weight: Double = links.iterator.map(_._2.toDouble).fold(0.0)(_ + _)

  def containRemote = formerlyRemoteLinks.isDefined

  /** @return an iterator over the ids of the neighbors of this node, and the weight of the link to said neighbor  */
  def neighbors (node: Int): Iterator[(Int, Float)] = {
    val allLinks = links.toList
    val first = firstNeighbor(node)
    val num = nb_neighbors(node)
    val neighbors = links.iterator.drop(first).take(num).toList
    links.iterator.drop(firstNeighbor(node)).take(nb_neighbors(node))
  }



  def nb_remote_neighbors (node: Int): Int = {
    assert(0 <= node && node < nb_nodes)
    formerlyRemoteLinks.map(_(node).size).getOrElse(0)
  }

  def remote_neighbors (node: Int): Option[Iterable[(Int, Float)]] = {
    assert(0 <= node && node < nb_nodes)
    formerlyRemoteLinks.map(_.get(node)).getOrElse(None)
  }

  def addFormerlyRemoteEdges (formerlyRemoteLinks: Map[Int, Seq[(Int, Float)]]) = {
    this.formerlyRemoteLinks = Some(formerlyRemoteLinks)

    total_weight = total_weight + formerlyRemoteLinks.map(_._2.map(_._2.toDouble).fold(0.0)(_ + _)).fold(0.0)(_ + _)
    nb_links = nb_links + formerlyRemoteLinks.map(_._2.size).fold(0)(_ + _)
  }

  /** @return the number of neighbors (degree) of the node */
  def nb_neighbors (node: Int): Int = {
    assert(0 <= node && node < nb_nodes)
    if (0 == node) degrees(node)
    else degrees(node) - degrees(node - 1)
  }

  /** @return a pointer to the first neighbor of the node */
  def firstNeighbor (node: Int): Int =
    if (0 == node) 0
    else degrees(node-1)

  /** @return the number of self loops of the node */
  def nb_selfloops (node: Int): Double = {
    assert(0 <= node && node < nb_nodes)
    neighbors(node).find(_._1 == node).map(_._2.toDouble).getOrElse(0.0)
  }

  /** @return the weighted degree of the node */
  def weighted_degree (node: Int): Double = {
    assert(0 <= node && node < nb_nodes)
    neighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
  }

  def weighted_degree_remote (node: Int): Double = {
    assert(0 <= node && node < nb_nodes)
    if (!containRemote) 0.0 else {
      remote_neighbors(node).map(_.map(_._2.toDouble).fold(0.0)(_ + _)).getOrElse(0.0)
    }
  }

  def checkSymetry: Boolean = {
    for (node <- 0 until nb_nodes) {
      neighbors(node).map { case (neighbor, weight) =>
        neighbors(neighbor).map { case (neighbor_neighbor, neighbor_weight) =>
          if (node == neighbor_neighbor && weight != neighbor_weight)
            throw new AsymetryException(node, neighbor, weight, neighbor_weight)
        }
      }
    }
    true
  }
}

class AsymetryException (node1: Int, node2: Int, weight1: Float, weight2: Float) extends Exception