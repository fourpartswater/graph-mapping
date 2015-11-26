package software.uncharted.graphing.clustering.usc.reference



import org.apache.spark.rdd.RDD

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}



/**
 * Basically, this is a combination of the LouvainMR and the ReduceCommunity classes in our reference implementation
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/LouvainMR.java
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/ReduceCommunity.java
 */
class LouvainSpark {
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

      Iterator(new GraphMessage(partition, graph, c))
    }

    firstPass.coalesce(1).mapPartitions{i =>
      val g = reconstructGraph(i)

      // TODO: Continue clustering
      
      null
    }
  }

  def reconstructGraph (i: Iterator[GraphMessage]): Graph = {
    val messages = i.toList.sortBy(_.partition)

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
}

