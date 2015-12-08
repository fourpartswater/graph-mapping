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



import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.graphx.VertexId



/**
 * A class to take the results of clustering on several subgraphs and consolidate them
 * Created by nkronenfeld on 12/7/2015.
 */
object GraphConsolidator {
  def apply[VD] (totalNodes: Int)(subGraphData: Iterator[(SubGraph[VD], Map[VertexId, VertexId])]): SubGraph[VD] = {
    val nodes = new Array[(VertexId, VD)](totalNodes)
    val links = new Array[Array[(Int, Float)]](totalNodes)
    val remoteLinks = new Array[Array[(VertexId, Float)]](totalNodes)
    val linkMapping = MutableMap[VertexId, VertexId]()
    val renumbering = MutableMap[VertexId, Int]()

    // We're just given an iterator, so we have to record all our input information in one run...
    // So we record data from disparate subgraphs, combining local link lists while we do so
    var n = 0
    subGraphData.foreach{ case (subGraph, localLinkMapping) =>
      val subGraphStart = n
      for (i <- 0 until subGraph.numNodes) {
        nodes(n) = subGraph.nodeData(i)
        renumbering(nodes(n)._1) = n
        links(n) = subGraph.internalNeighbors(i).map{case (internalId, weight) => (internalId + subGraphStart, weight)}.toArray
        remoteLinks(n) = subGraph.externalNeighbors(i).toArray
        n = n + 1
      }
      linkMapping ++= localLinkMapping
    }

    // Now we take the remote links, convert them to local links, and merge them into the local link list (leaving the
    // remote link lists empty)
    for (i <- 0 until totalNodes) {
      val localNodeLinks = links(i)
      val numLocalLinks = localNodeLinks.size
      val remoteNodeLinks = remoteLinks(i)
      val numRemoteLinks = remoteNodeLinks.size

      // Consolidate remote links - several links may go to what is now the same node, and we want to combine those.
      val consolidatedRemoteNodeLinks = MutableMap[Int, Float]()
      for (j <- 0 until numRemoteLinks) {
        val (origNodeId, weight) = remoteNodeLinks(j)
        linkMapping.get(origNodeId) match {
          case Some(newNodeId) => {
            renumbering.get(newNodeId) match {
              case Some(newNodeIndex) => {
                consolidatedRemoteNodeLinks(newNodeIndex) = consolidatedRemoteNodeLinks.get(newNodeIndex).getOrElse(0.0f) + weight
              }
              case None => {
                println("\nCouldn't translate "+origNodeId+", which has changed to "+newNodeId+": Not found in renumbering map")
              }
            }
          }
          case None => {
            println("\nCouldn't find "+origNodeId+" in link mapping")
          }
        }
      }
      val numConsolidatedRemoteLinks = consolidatedRemoteNodeLinks.size

      // Put the two lists together into one list of all (now local) links
      val allNodeLinks = new Array[(Int, Float)](numLocalLinks + numConsolidatedRemoteLinks)
      for (j <- 0 until numLocalLinks) {
        allNodeLinks(j) = localNodeLinks(j)
      }
      var j = numLocalLinks
      consolidatedRemoteNodeLinks.foreach { link =>
        allNodeLinks(j) = link
        j = j + 1
      }

      // Set local links to combined list
      links(i) = allNodeLinks
      // Set remote links to empty list
      remoteLinks(i) = Array[(VertexId, Float)]()
    }

    new SubGraph(nodes, links, remoteLinks)
  }
}
