package software.uncharted.graphing.clustering.usc.reference


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph => SparkGraph}
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.PartitionID
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}

import software.uncharted.graphing.clustering.ClusteringStatistics
import software.uncharted.spark.ExtendedRDDOpertations._


/**
 * Basically, this is a combination of the LouvainMR and the ReduceCommunity classes in our reference implementation
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/LouvainMR.java
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/ReduceCommunity.java
 */
object LouvainSpark {
  /**
   * Run the complete USC version of Louvain clustering
   * @param input An RDD of graph objects, one per partition
   */
  def doClustering (numPasses: Int, minModularityIncrease: Double, randomize: Boolean)(input: RDD[Graph]) = {
    val firstPass = input.mapPartitionsWithIndex { case (partition, index) =>
      val graph = index.next()
      val c = new Community(graph, numPasses, minModularityIncrease)
      val startModularity = c.modularity
      c.one_level(randomize)
      val endModularity = c.modularity
      val endGraph = c.partition2graph_binary

      val stats = c.clusteringStatistics.map(cs => cs.addLevelAndPartition(1, partition))
      Iterator((new GraphMessage(partition, endGraph, c), stats))
    }

    val (graph, stats) = firstPass.coalesce(1).mapPartitions{i =>
      val precision = 0.000001
      val stats = Buffer[ClusteringStatistics]()
      val g = reconstructGraph(i, stats)
      var g2 = g
      var c  = new Community(g, -1, precision)

      // First pass is done; do the rest of the clustering
      var modularity = c.modularity
      var new_modularity = modularity
      var improvement = true
      var level = 2

      do {
        improvement = c.one_level()
        new_modularity = c.modularity
        level = level + 1
        g2 = c.partition2graph_binary
        c.clusteringStatistics.map(cs =>
          stats += cs.addLevelAndPartition(level, -1)
        )

        c = new Community(g2, -1, precision)
        modularity = new_modularity
      } while (improvement || level < 4)

      Iterator((g2, stats.toArray))
    }.collect.head
    (graph, stats)
  }

  def reconstructGraph (i: Iterator[(GraphMessage, Option[ClusteringStatistics])], stats: Buffer[ClusteringStatistics]): Graph = {
    val messages = Buffer[GraphMessage]()
    i.foreach{case (message, msgStatsOption) =>
        messages += message
        msgStatsOption.map(msgStats => stats += msgStats)
    }

    var gap = 0
    var degreeGap = 0

    messages.foreach{gm =>
      if (gap > 0) {
        for (i <- 0 until gm.links.size) gm.links(i) = (gm.links(i)._1 + gap, gm.links(i)._2)
        gm.remoteMaps.map(remoteMaps =>
          for (i <- 0 until remoteMaps.size) remoteMaps(i).source = remoteMaps(i).source + gap
        )
        for (i <- 0 until gm.nodeToCommunity.size) gm.nodeToCommunity(i) = gm.nodeToCommunity(i) + gap
        for (i <- 0 until gm.degrees.size) gm.degrees(i) = gm.degrees(i) + degreeGap
      }

      gap = gap + gm.numNodes
      degreeGap = gm.degrees.last
    }

    // Merge local portions
    val graph = new Graph(
      messages.flatMap(_.degrees.toSeq),
      messages.flatMap(_.links.toSeq),
      None
    )

    // Merge remote portions
    val remoteLinks = MutableMap[Int, Buffer[(Int, Float)]]()
    messages.foreach{message =>
      val m = MutableMap[(Int, Int), Float]()
      message.remoteMaps.foreach{remoteMaps =>
        remoteMaps.foreach { remoteMap =>
          val key = (remoteMap.source, messages(remoteMap.sinkPart).nodeToCommunity(remoteMap.sink))
          m(key) = m.get(key).getOrElse(0.0f) + 1.0f
        }
      }

      // set graph.numLinks to graph.numLinks + m.size
      m.foreach{case (key, w) =>
        val linkBuffer = remoteLinks.get(key._1).getOrElse(Buffer[(Int, Float)]())
        linkBuffer += ((key._1, w))
        remoteLinks(key._1) = linkBuffer
      }
    }
    graph.addFormerlyRemoteEdges(remoteLinks.map{case (source, partitionCommunities) => (source, partitionCommunities.toSeq)}.toMap)

    graph
  }

  def sparkGraphToUSCGraphs[VD, ED] (graph: SparkGraph[VD, ED],
                                     getEdgeWeight: Option[ED => Float],
                                     partitions: Int)
                                    (implicit order: Ordering[(VertexId, VD)]): RDD[Graph] = {
    val sc = graph.vertices.context

    // Order our vertices
    val orderedVertices: RDD[(VertexId, VD)] = graph.vertices.sortBy(v => v)
    orderedVertices.cache

    // Repartition the vertices
    val repartitionedVertices = orderedVertices.repartitionEqually(partitions)

    // Get the vertex boundaries of each partition, so we can repartition the edges the same way
    val partitionBoundaries = sc.broadcast(
      repartitionedVertices.mapPartitionsWithIndex { case (partition, elements) =>
      val bounds = elements.map { case (vertexId, vertexData) =>
        (vertexId, vertexId)
      }.reduce((a, b) => (a._1 min b._1, a._2 max b._2))

      Iterator((partition, bounds))
    }.collect.toMap
    )

    // Repartition the edges using these partition boundaries - collect all edges of node X into the partition of the
    // EdgeRDD corresponding to the partition of the VertexRDD containing node X
    val repartitionedEdges = graph.partitionBy(
      new SourcePartitioner(partitionBoundaries),
      repartitionedVertices.partitions.size
    ).edges

    // Combine the two into an RDD of subgraphs, one per partition
    repartitionedVertices.zipPartitions(repartitionedEdges, true) { case (vi, ei) =>
      val nodes = vi.toArray
      val numNodes = nodes.size
      val newIdByOld = MutableMap[Long, Int]()

      // Renumber the nodes (so that the IDs are integers, since the BGLL algorithm doesn't work in scala with Long
      // node ids)
      for (i <- 0 until numNodes) {
        newIdByOld(nodes(i)._1) = i
      }

      // Separate edges into internal and external edges
      val internalLinks = new Array[Buffer[(Int, Float)]](numNodes)
      val externalLinks = new Array[(Int, Buffer[(VertexId, Float)])](numNodes)
      for (i <- 0 until numNodes) {
        internalLinks(i) = Buffer[(Int, Float)]()
        externalLinks(i) = (i, Buffer[(VertexId, Float)]())
      }

      ei.foreach{edge =>
        val srcIdOric = edge.srcId
        val srcId = newIdByOld(srcIdOric)
        val dstIdOrig = edge.dstId
        val weight: Float = getEdgeWeight.map(_(edge.attr)).getOrElse(1.0f)
        if (newIdByOld.contains(dstIdOrig)) {
          internalLinks(srcId) += ((newIdByOld(dstIdOrig), weight))
        } else {
          externalLinks(srcId)._2 += ((dstIdOrig, weight))
        }
      }

      // Get our cumulative degree arrays
      val degrees: Array[Int] = new Array[Int](numNodes)
      degrees(0) = internalLinks(0).size
      if (numNodes > 1) {
        for (i <- 1 until numNodes) {
          degrees(i) = degrees(i - 1) + internalLinks(i).size
        }
      }

      // Put all internal links into the form we need
      val links = internalLinks.flatMap(linksForNode => linksForNode)

      // Put all external links into the form we need
      val remoteLinks =
        externalLinks.flatMap{case (source, sinks) =>
          sinks.map{case (vertexId, weight) =>
            val partition = partitionBoundaries.value.find{case (partition, (min, max)) =>
              min <= vertexId && vertexId <= max
            }.get._1
            val indexInPartition = (vertexId - partitionBoundaries.value(partition)._1).toInt
            new RemoteMap(source, indexInPartition, partition)
          }
        }

      // degrees: Seq[Int]
      // links: Seq[(Int, Float)]
      // remoteLinks: Option[Seq[RemoteMap]]
      Iterator(new Graph(degrees, links, Some(remoteLinks)))
    }
  }
}

class SourcePartitioner (boundaries: Broadcast[Map[Int, (Long, Long)]]) extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    boundaries.value.filter { case (partition, (minVertex, maxVertex)) =>
      minVertex <= src && src <= maxVertex
    }.head._1
  }
}
