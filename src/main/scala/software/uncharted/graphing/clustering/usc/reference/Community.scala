//scalastyle:off
package software.uncharted.graphing.clustering.usc.reference


import software.uncharted.graphing.clustering.ClusteringStatistics

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}
import scala.util.Random


/**
 * Adapted from
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/Community.java
 *
 * Some small changes:
 *    * We assume all graphs are weighted.  Just use weights of 1.0f if you don't have any inherently.
 *    * because of that, we can store links and weights in the same structure.
 *    * We also store all per-node information in a single object (NodeInfo)
 *             neigh_pos => position
 *          neigh_weight => weight
 *                   n2c => community
 *                    in => internalWeight
 *                   tot => totalWeight
 */
class Community (g: Graph, nb_pass: Int, min_modularity: Double) {
  private var size = g.nb_nodes
  private val nodeInfos = Array.tabulate(size)(i => new NodeInfo(i, g.nb_selfloops(i), g.weighted_degree(i)))

  // Used for calculation of new communities
  private val neigh_pos = Array.fill(size)(0)
  private val neigh_weight = Array.fill(size)(-1.0)
  private var neigh_last = 0

  val newCommunities = new Vector[Int](0)

  var clusteringStatistics: Option[ClusteringStatistics] = None

  def remove (node: Int, community: Int, dnodecomm: Double): Unit = {
    assert(0 <= node && node < size)

    nodeInfos(community).totalWeight = nodeInfos(community).totalWeight - g.weighted_degree(node) - g.weighted_degree_remote(node)
    nodeInfos(community).internalWeight = nodeInfos(community).internalWeight - (2 * dnodecomm + g.nb_selfloops(node))
    nodeInfos(node).community = -1
  }

  def insert (node: Int, community: Int, dnodecomm: Double): Unit = {
    assert(0 <= node && node < size)

    nodeInfos(community).totalWeight = nodeInfos(community).totalWeight + g.weighted_degree(node) + g.weighted_degree_remote(node)
    nodeInfos(community).internalWeight = nodeInfos(community).internalWeight + (2 * dnodecomm + g.nb_selfloops(node))
    nodeInfos(node).community = community
  }

  def modularity_gain (node: Int, community: Int, dnodecomm: Double, w_degree: Double): Double = {
    assert(0 <= node && node < size)

    val totc = nodeInfos(community).totalWeight
    val degc = w_degree
    val m2 = g.total_weight
    val dnc = dnodecomm

    (dnc - totc * degc / m2)
  }

  private def updateNode (node: Int, neigh: Int, neigh_w: Double): Unit = {
    val neigh_comm = nodeInfos(neigh).community
    if (node != neigh) {
      if (-1 == neigh_weight(neigh_comm)) {
        neigh_weight(neigh_comm) = 0.0
        neigh_pos(neigh_last) = neigh_comm
        neigh_last = neigh_last + 1
      }
      neigh_weight(neigh_comm) = neigh_weight(neigh_comm) + neigh_w
    }
  }

  def neigh_comm (node: Int): Unit = {
    for (i <- 0 until neigh_last) {
      neigh_weight(neigh_pos(i)) = -1.0
    }
    neigh_last = 0

    val degree = g.nb_neighbors(node)

    neigh_pos(0) = nodeInfos(node).community
    neigh_weight(neigh_pos(0)) = 0.0
    neigh_last = 1

    g.neighbors(node).foreach { case (neigh, neigh_w) =>
      updateNode(node, neigh, neigh_w)
    }

    g.remote_neighbors(node).map(_.foreach { case (neigh, neigh_w) =>
      updateNode(node, neigh, neigh_w)
    })
  }

  def modularity: Double = {
    val m2 = g.total_weight

    nodeInfos.iterator.map{node =>
      if (node.totalWeight > 0.0) {
        (node.internalWeight /m2) - (node.totalWeight / m2)*(node.totalWeight) / m2
      } else 0.0
    }.fold(0.0)(_ + _)
  }

  def partition2graph_binary: Graph = {
    val renumber = Array.fill(size)(-1)
    for (i <- 0 until size) renumber(nodeInfos(i).community) = 1
    var fin = 0
    for (i <- 0 until size) {
      if (-1 != renumber(i)) {
        renumber(i) = fin
        fin = fin + 1
      }
    }
    // Map from new communities to old
    val comm_nodes = Array.fill(fin)(Buffer[Int]())
    // Map from old communities to new
    newCommunities.clear

    for (node <- 0 until size) {
      val community = renumber(nodeInfos(node).community)
      comm_nodes(community) += node
      newCommunities(node) = community
    }

    // Compute weighted graph
    val comm_deg = comm_nodes.size
    val degrees = Buffer[Int]()
    val links = Buffer[(Int, Float)]()
    for (comm <- 0 until comm_deg) {
      val m = MutableMap[Int, Double]()
      val comm_size = comm_nodes(comm).size
      for (nodeInComm <- 0 until comm_size) {
        val node = comm_nodes(comm)(nodeInComm)

        g.neighbors(node).foreach { case (neigh, neigh_weight) =>
          val neigh_comm = renumber(nodeInfos(neigh).community)
          m(neigh_comm) = m.get(neigh_comm).getOrElse(0.0) + neigh_weight
        }

        g.remote_neighbors(node).foreach(_.map { case (neigh, neigh_weight) =>
          val neigh_comm = renumber(nodeInfos(neigh).community)
          m(neigh_comm) = m.get(neigh_comm).getOrElse(0.0) + neigh_weight
        })
      }

      degrees += (if (0 == comm) m.size else m.size + degrees.last)

      m.foreach { case (node, weight) =>
        links += ((node, weight.toFloat))
      }
    }

    val result = new Graph(degrees, links, g.remoteLinks)

    // Add nodes and links to clustering statistics, if there are any
    clusteringStatistics = clusteringStatistics.map(cs =>
      ClusteringStatistics(
        cs.level, cs.partition, cs.iterations,
        cs.startModularity, cs.startNodes, cs.startLinks,
        cs.endModularity, result.nb_nodes, result.nb_links,
        cs.timeToCluster
      )
    )
    result
  }

  def one_level (randomize: Boolean = true): Boolean = {
    val startNodes = g.nb_nodes
    val startLinks = g.nb_links
    val startModularity = modularity
    val startTime = System.currentTimeMillis()
    var iterations = 0

    var improvement = false
    var nb_moves = 0
    var nb_pass_done = 0
    var new_mod = modularity
    var cur_mod = new_mod

    val random_order: Array[Int] =
      if (randomize) (new Random).shuffle((0 until size).toList).toArray
      else Array.tabulate(size)(n => n)

    // Repeat while
    //   there is an improvement in modularity
    //   or there is an improvement of modularity greater than a given epsilon
    //   or a predefined number of passes have been done
    do {
      cur_mod = new_mod
      nb_moves = 0
      nb_pass_done = nb_pass_done + 1

      // For each node: remove the node from its community and insert it in the best community
      for (node_tmp <- 0 until size) {
        val node = random_order(node_tmp)
        val node_comm = nodeInfos(node).community
        val w_degree = g.weighted_degree(node) + g.weighted_degree_remote(node)

        // computation of all neighboring communities of current node
        neigh_comm(node)

        // remove node from its current community
        remove(node, node_comm, neigh_weight(node_comm))

        // compute the nearest community for node
        // default choice for future insertion is the former community
        var best_comm = node_comm
        var best_nblinks = 0.0
        var best_increase = 0.0
        for (i <- 0 until neigh_last) {
          val increase = modularity_gain(node, neigh_pos(i), neigh_weight(neigh_pos(i)), w_degree)
          if (increase > best_increase) {
            best_comm = neigh_pos(i)
            best_nblinks = neigh_weight(best_comm)
            best_increase = increase
          }
        }

        // insert the node in the nearest community
        insert(node, best_comm, best_nblinks)

        if (best_comm != node_comm) nb_moves = nb_moves + 1
      }

      new_mod = modularity
      iterations = iterations + 1
      if (nb_moves > 0) improvement = true
    } while (nb_moves > 0 && (new_mod - cur_mod) > min_modularity)
//  } while (nb_moves > 0 && (new_mod - cur_mod).abs > min_modularity)

    // Deal with recording the end nodes and end links when we calculate the new graph
    val endNodes = 0
    val endLinks = 0L
    val endModularity = new_mod
    val endTime = System.currentTimeMillis()

    clusteringStatistics = Some(ClusteringStatistics(
      -1, -1, iterations,
      startModularity, startNodes, startLinks,
      endModularity, endNodes, endLinks,
      endTime - startTime
    ))

    improvement
  }
}



case class NodeInfo (var community: Int,
                     var internalWeight: Double,
                     var totalWeight: Double)
//scalastyle:on
