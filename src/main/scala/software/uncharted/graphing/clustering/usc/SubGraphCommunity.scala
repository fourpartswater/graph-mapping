/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.clustering.usc


import org.apache.spark.graphx.VertexId

import scala.collection.mutable.{Buffer, Map => MutableMap}
import scala.util.Random



/**
 * A class to run Louvain clustering on a sub-graph
 * @param sg The graph to cluster
 * @param numPasses number of pass for one level computation if -1, compute as many pass as needed to increas
 *                  modularity
 * @param minModularity a new pass is computed if the last one has generated an increase greater than min_modularity
 *                      if 0, even a minor increase is enough to go for one more pass
 */
class SubGraphCommunity[VD] (val sg: SubGraph[VD], numPasses: Int, minModularity: Double) {
  val size = sg.numNodes
  val n2c = (0 until size).toArray
  val tot = n2c.map(n => sg.weightedInternalDegree(n))
  val in = n2c.map(n => sg.weightedSelfLoopDegree(n))
  val neigh_weight = n2c.map(n => -1.0)
  val neigh_pos = n2c.map(n => 0)
  var neigh_last: Int = 0


  def remove(node: Int, comm: Int, dnodecomm: Double): Unit = {
    tot(comm) = tot(comm) - sg.weightedInternalDegree(node)
    in(comm) = in(comm) - (2 * dnodecomm + sg.weightedSelfLoopDegree(node))
    n2c(node) = -1
  }

  def insert(node: Int, comm: Int, dnodecomm: Double): Unit = {
    tot(comm) = tot(comm) + sg.weightedInternalDegree(node)
    in(comm) = in(comm) + (2 * dnodecomm + sg.weightedSelfLoopDegree(node))
    n2c(node) = comm
  }

  def modularity_gain(node: Int, comm: Int, dnodecomm: Double, w_degree: Double): Double = {
    val totc = tot(comm)
    val degc = w_degree
    val m2 = sg.totalInternalWeight
    val dnc = dnodecomm
    (dnc - totc * degc / m2)
  }

  def modularity: Double = {
    var q = 0.0
    val m2 = sg.totalInternalWeight

    for (i <- 0 until size) {
      if (tot(i) > 0) {
        val tm = tot(i) / m2
        q += in(i) / m2 - tm * tm
      }
    }

    q
  }

  def neigh_comm(node: Int): Unit = {
    for (i <- 0 until neigh_last) {
      neigh_weight(neigh_pos(i)) = -1
    }
    val degree = sg.numInternalNeighbors(node)
    neigh_pos(0) = n2c(node)
    neigh_weight(neigh_pos(0)) = 0
    neigh_last = 1
    val neighbors = sg.internalNeighbors(node)
    for (i <- 0 until degree) {
      val (neighbor, neighbor_weight) = neighbors.next
      val neighbor_community = n2c(neighbor)
      if (neighbor != node) {
        if (neigh_weight(neighbor_community) == -1.0) {
          neigh_weight(neighbor_community) = 0.0
          neigh_pos(neigh_last) = neighbor_community
          neigh_last += 1
        }
        neigh_weight(neighbor_community) += neighbor_weight
      }
    }
  }

  def one_level (randomize: Boolean = true): Boolean = {
    var improvement = false
    var nb_moves: Int = 0
    var nb_pass_done: Int = 0
    var new_mod = modularity
    var cur_mod = new_mod

    // Randomize the order of vertex inspection
    val random_order = (0 until size).toArray
    val randomizer = new Random()
    if (randomize) {
      for (i <- 0 until (size - 1)) {
        val rand_pos = randomizer.nextInt(size - i) + i
        val tmp = random_order(i)
        random_order(i) = random_order(rand_pos)
        random_order(rand_pos) = tmp
      }
    }

    // repeat while
    //   there is an improvement of modularity
    //   or there is an improvement of modularity greater than a given epsilon
    //   or a predefined number of pass have been done
    do {
      cur_mod = new_mod
      nb_moves = 0
      nb_pass_done += 1

      // for each node: remove the node from its community and insert it in the best community
      for (node_tmp <- 0 until size) {
        val node = random_order(node_tmp)
        val node_comm = n2c(node)
        val w_degree = sg.weightedInternalDegree(node)

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
            best_nblinks = neigh_weight(neigh_pos(i))
            best_increase = increase
          }
        }

        // insert node in the nearest community
        insert(node, best_comm, best_nblinks)

        if (best_comm != node_comm)
          nb_moves += 1
      }

      var total_tot = 0.0
      var total_in = 0.0
      for (i <- 0 until tot.size) {
        total_tot += tot(i)
        total_in += in(i)
      }

      new_mod = modularity
      if (nb_moves > 0)
        improvement = true;

    } while (nb_moves > 0 && new_mod - cur_mod > minModularity)

    improvement
  }

  private def getRenumbering: (Array[Int], Int) = {
    val renumber = (0 until size).map(n => -1).toArray
    for (node <- 0 until size)
      renumber(n2c(node)) += 1

    var last = 0
    for (i <- 0 until size) {
      if (renumber(i) != -1) {
        renumber(i) = last
        last += 1
      }
    }

    (renumber, last)
  }

  /**
   * Get the mapping created by our clustering from old vertex IDs to new ones.
   * @return
   */
  def getVertexMapping: Map[VertexId, VertexId] = {
    Range(0, size).iterator.map { node =>
      val newCommunity = n2c(node)
      val oldVertexId = sg.nodeData(node)._1
      val newVertexId = sg.nodeData(newCommunity)._1
      (oldVertexId, newVertexId)
    }.toMap
  }

  /**
   * Get the reduced subgraph according to the current state of clustering
   * @param mergeNodeData A function to merge the data from the nodes in the original graph from which our clustering
   *                      is derived. The default implementation is simply to take the data from the node with the
   *                      highest original degree.
   * @return
   */
  def getReducedSubgraph (mergeNodeData: (VD, VD) => VD = (a, b) => a): SubGraph[VD] = {
    val (renumbering, newSize) = getRenumbering

    // For each new community, we need:
    //  node id
    //  node data
    //  internal links
    //  external links
    //  internal link weights
    //  external link weights
    // Note that weights are not optional; we are combining old links, so even if they were originally optional,
    // we consider the original links to have a weight of 1 (so total weight was degree), and we now need to
    // aggregate those weights to differentiate strongly linked communities from weakly linked ones.
    val nodeInfos = new Array[(VertexId, VD)](newSize)
    val highestOriginalDegree = new Array[Double](newSize) // Keep track of the most important node in each cluster,
    // so we can renumber to that one.
    val internalLinks = new Array[MutableMap[Int, Float]](newSize)
    val externalLinks = new Array[MutableMap[VertexId, Float]](newSize)
    for (node <- 0 until newSize) {
      internalLinks(node) = MutableMap[Int, Float]()
      externalLinks(node) = MutableMap[VertexId, Float]()
    }

    for (node <- 0 until size) {
      val newCommunity = renumbering(n2c(node))
      val weight = sg.weightedInternalDegree(node)

      // Add node info from this node into the new community
      val (oldNodeId, nodeData) = sg.nodeData(node)
      if (null == nodeInfos(newCommunity)) {
        highestOriginalDegree(newCommunity) = weight
        nodeInfos(newCommunity) = (oldNodeId, nodeData)
      } else {
        val (curId, curData) = nodeInfos(newCommunity)
        if (highestOriginalDegree(newCommunity) < weight) {
          highestOriginalDegree(newCommunity) = weight
          nodeInfos(newCommunity) = (oldNodeId, mergeNodeData(nodeData, curData))
        } else {
          nodeInfos(newCommunity) = (curId, mergeNodeData(curData, nodeData))
        }
      }

      // Add links and weights from this node into the new community
      sg.internalNeighbors(node).foreach { case (oldNeighborId, weight) =>
        val newNeighborId = renumbering(n2c(oldNeighborId))
        internalLinks(newCommunity)(newNeighborId) = internalLinks(newCommunity).get(newNeighborId).getOrElse(0.0f) + weight
      }
      sg.externalNeighbors(node).foreach { case (oldNeighborId, weight) =>
        externalLinks(newCommunity)(oldNeighborId) = externalLinks(newCommunity).get(oldNeighborId).getOrElse(0.0f) + weight
      }
    }


    new SubGraph(nodeInfos, internalLinks.map(_.toArray), externalLinks.map(_.toArray))
  }
}
