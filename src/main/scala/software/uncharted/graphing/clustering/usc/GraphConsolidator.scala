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
      val allNodeLinks = new Array[(Int, Float)](localNodeLinks.size + remoteNodeLinks.size)
      for (j <- 0 until numLocalLinks) {
        allNodeLinks(j) = localNodeLinks(j)
      }
      for (j <- 0 until numRemoteLinks) {
        val (origNodeId, weight) = remoteNodeLinks(j)
        // Map from original node ID to the node ID of the community into which it has moved
        val newNodeId  = linkMapping(origNodeId)
        // Map from the community node ID to the local index of that community
        val newNodeIndex = renumbering(newNodeId)

        allNodeLinks(j + numLocalLinks) = (newNodeIndex, weight)
      }

      // Set local links to combined list
      links(i) = allNodeLinks
      // Set remote links to empty list
      remoteLinks(i) = Array[(VertexId, Float)]()
    }

    new SubGraph(nodes, links, remoteLinks)
  }
}
