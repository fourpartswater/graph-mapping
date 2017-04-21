//scalastyle:off
/**
  * Code is an adaptation of https://github.com/usc-cloud/hadoop-louvain-community with the original done by
  * Copyright 2013 University of California, licensed under the Apache License, version 2.0,
  * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0
  *
  * There are some minor fixes, which I have attempted to resubmit back to the baseline version.
  */
package software.uncharted.graphing.clustering.usc.reference

class GraphMessage (val partition: Int,
                    val degrees: Array[Int],
                    val links: Array[(Int, Float)],
                    val totalWeight: Double,
                    val nodeToCommunity: Array[Int],
                    val remoteMaps: Option[Seq[RemoteMap]]) extends Serializable {
  val numNodes = degrees.size
  val numLinks = links.size

  def this(partition: Int, graph: Graph, community: Community) =
    this(
      partition,
      graph.degrees.toArray,
      graph.links.toArray,
      graph.total_weight,
      community.newCommunities.toArray,
      graph.remoteLinks
    )
}
//scalastyle:on
