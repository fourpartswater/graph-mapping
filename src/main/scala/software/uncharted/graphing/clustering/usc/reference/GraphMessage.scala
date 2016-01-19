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
