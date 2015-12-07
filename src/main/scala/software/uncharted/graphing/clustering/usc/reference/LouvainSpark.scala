package software.uncharted.graphing.clustering.usc.reference



import java.util.Date

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph => SparkGraph, Edge, PartitionStrategy, PartitionID, VertexId}
import org.apache.spark.rdd.RDD

import com.oculusinfo.tilegen.util.ArgumentParser

import software.uncharted.graphing.clustering.ClusteringStatistics
import software.uncharted.spark.ExtendedRDDOpertations._
import software.uncharted.graphing.utilities.GraphOperations
import GraphOperations._



/**
 * Basically, this is a combination of the LouvainMR and the ReduceCommunity classes in our reference implementation
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/LouvainMR.java
 * https://github.com/usc-cloud/hadoop-louvain-community/blob/master/src/main/java/edu/usc/pgroup/louvain/hadoop/ReduceCommunity.java
 */
object LouvainSpark {
  def usage: Unit = {
    println("Usage: LouvainSpark <node file> <node prefix> <edge file> <edge prefix> <partitions>")
  }

  def parseLine[TA] (separator: String, columnA: Int, conversionA: String => TA): String => TA = line => {
    val fields = line.split(separator)
    conversionA(fields(columnA))
  }

  def parseLine[TA, TB] (separator: String,
                         columnA: Int, conversionA: String => TA,
                         columnB: Int, conversionB: String => TB): String => (TA, TB) = line => {
    val fields = line.split(separator)
    (conversionA(fields(columnA)), conversionB(fields(columnB)))
  }

  def parseLine[TA, TB, TC] (separator: String,
                             columnA: Int, conversionA: String => TA,
                             columnB: Int, conversionB: String => TB,
                             columnC: Int, conversionC: String => TC): String => (TA, TB, TC) = line => {
    val fields = line.split(separator)
    (conversionA(fields(columnA)), conversionB(fields(columnB)), conversionC(fields(columnC)))
  }

  def getData[T: ClassTag] (sc: SparkContext, source: String, prefixOpt: Option[String], parser: String => T): RDD[T] = {
    val rawSource = sc.textFile(source)
    val filteredSource = prefixOpt.map(prefix => rawSource.filter(_.startsWith(prefix))).getOrElse(rawSource)
    filteredSource.map{line => Try(parser(line))}.filter(_.isSuccess).map(_.get)
  }

  def main (args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argParser = new ArgumentParser(args)

    val (nodeFile, nodePrefix, nodeSeparator, nodeIdCol, edgeFile, edgePrefix, edgeSeparator, edgeSrcCol, edgeDstCol, weightColOpt, partitions):
    (String, Option[String], String, Int, String, Option[String], String, Int, Int, Option[Int], Int) =
      try {
        val nodeFile = argParser.getString("nodeFile", "The data file from which to get nodes")
        val nodePrefix = argParser.getStringOption("nodePrefix", "A prefix required on every line of the node data file for a line to count as a node.", Some("node"))
        val nodeSeparator = argParser.getString("nodeSeparator", "A separator string for breaking the node data file into columns", Some("\t"))
        val nodeIdCol = argParser.getInt("nodeIdCol", "The column number of the column of node lines containing the node ID (which must be parsable into a long)")

        val edgeFile = argParser.getString("edgeFile", "The data file from which to get edges")
        val edgePrefix = argParser.getStringOption("edgePrefix", "A prefix required on every line of the edge data file for a line to count as a edge.", Some("edge"))
        val edgeSeparator = argParser.getString("edgeSeparator", "A separator string for breaking the edge data file into columns", Some("\t"))
        val edgeSrcCol = argParser.getInt("edgeSrcCol", "The column number of the column of edge lines containing the node ID of the source node")
        val edgeDstCol = argParser.getInt("edgeDstCol", "The column number of the column of edge lines containing the node ID of the destination node")
        val weightColOpt = argParser.getIntOption("edgeWeightCol", "The column number of the column of edge lines containing the weight of the edge")

        val partitions = argParser.getInt("partitions", "The number of partitions into which to break the graph for first-round processing")
        (nodeFile, nodePrefix, nodeSeparator, nodeIdCol, edgeFile, edgePrefix, edgeSeparator, edgeSrcCol, edgeDstCol, weightColOpt, partitions)
      } catch {
        case e: Exception => {
          argParser.usage
          return
        }
      }

    val sc = new SparkContext((new SparkConf).setAppName("USC Louvain Clustering"))

    val nodes: RDD[(Long, Long)] = getData(
      sc, nodeFile, nodePrefix,
      parseLine(nodeSeparator, nodeIdCol, _.toLong)
    ).map(n => (n, n))

    val edges: RDD[Edge[Float]] = weightColOpt.map(weightCol =>
      getData(
        sc, edgeFile, edgePrefix,
        parseLine(edgeSeparator, edgeSrcCol, _.toLong, edgeDstCol, _.toLong, weightCol, _.toFloat)
      ).map { case (src, dst, weight) => new Edge(src, dst, weight) }
    ).getOrElse(
        getData(
          sc, edgeFile, edgePrefix,
          parseLine(edgeSeparator, edgeSrcCol, _.toLong, edgeDstCol, _.toLong)
        ).map { case (src, dst) => new Edge(src, dst, 1.0f) }
      )
    val sparkGraph = SparkGraph(nodes, edges).explicitlyBidirectional(f => f).renumber()
    val uscGraph = sparkGraphToUSCGraphs(sparkGraph, Some((weight: Float) => weight), partitions)
    uscGraph.mapPartitionsWithIndex{case (partition, graphs) =>
      graphs.map(graph => (partition, graph))
    }.foreach { case (partition, graph) =>
      println("Partition " + partition + " constructed with: ")
      println("\tnodes: " + graph.nb_nodes)
      println("\tlinks: " + graph.nb_links)
      println("\tRemote links: " + graph.remoteLinks.map(_.size).getOrElse(0))
      println("\tTimestamp: "+new Date())
    }
    val (resultGraph, stats) = doClustering(-1, 0.15, false)(uscGraph)
    stats.foreach(println)
  }

  /**
   * Run the complete USC version of Louvain clustering
   * @param input An RDD of graph objects, one per partition
   */
  def doClustering (numPasses: Int, minModularityIncrease: Double, randomize: Boolean)(input: RDD[Graph]) = {
    println("Starting first pass on "+input.partitions.size+" partitions")
    val firstPass = input.mapPartitionsWithIndex { case (partition, index) =>
      def logStat (stat: String, value: String) =
	      println("\tpartition "+partition+": "+stat+": "+value+"\t\t"+new Date())

      logStat("first pass", "start")
      val g1 = index.next()
      logStat("first pass", "graph")
      val c1 = new Community(g1, numPasses, minModularityIncrease)
      logStat("nodes 1", g1.nb_nodes.toString)
      logStat("links 1", g1.nb_links.toString)
      logStat("modul 1", c1.modularity.toString)
      c1.one_level(randomize)
      val g2 = c1.partition2graph_binary
      val s1 = c1.clusteringStatistics.map(cs => cs.addLevelAndPartition(1, partition))
      logStat("first pass", "complete")

      Iterator((new GraphMessage(partition, g2, c1), s1.toIterable))
    }

    val (graph, stats) = firstPass.repartition(1).mapPartitions{i =>
      def logStat (stat: String, value: String, level: Int) =
	      println("\tlevel "+level+": "+stat+": "+value+"\t\t"+new Date())

      val precision = 0.000001
      val stats = Buffer[ClusteringStatistics]()
      logStat("consolidation", "start", 0)
      val g = reconstructGraph(i, stats)
      logStat("consolidation", "reconstruction", 0)

      var g2 = g
      var c  = new Community(g, -1, precision)
      logStat("nodes", g.nb_nodes.toString, 0)
      logStat("links", g.nb_links.toString, 0)
      logStat("modul", c.modularity.toString, 0)

      // First pass is done; do the rest of the clustering
      var modularity = c.modularity
      var new_modularity = modularity
      var improvement = true
      var level = 2

      do {
        logStat("consolidation", "start", level)
        improvement = c.one_level()
        new_modularity = c.modularity
        level = level + 1
        logStat("consolidation", "clustered", level)
        g2 = c.partition2graph_binary
        logStat("consolidation", "consolidated", level)
        c.clusteringStatistics.map { cs =>
          val levelStats = cs.addLevelAndPartition(level, -1)
          println("\tLevel stats: "+levelStats)
          stats += levelStats
        }

        logStat("consolidation", "done", level)
        c = new Community(g2, -1, precision)
        logStat("nodes", g2.nb_nodes.toString, level)
        logStat("links", g2.nb_links.toString, level)
        logStat("modul", c.modularity.toString, level)
        modularity = new_modularity
      } while (improvement || level < 4)

      logStat("consolidation", "all complete", 0)
      Iterator((g2, stats.toArray))
    }.collect.head
    (graph, stats)
  }

  def reconstructGraph (i: Iterator[(GraphMessage, Iterable[ClusteringStatistics])], stats: Buffer[ClusteringStatistics]): Graph = {
    val messages = Buffer[GraphMessage]()
    var msgId = 0
    i.foreach { case (message, msgStatsSeq) =>
      messages += message
      msgStatsSeq.foreach(msgStats => stats += msgStats)
      msgId = msgId + 1
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
	        try {
		        val key = (remoteMap.source, messages(remoteMap.sinkPart).nodeToCommunity(remoteMap.sink))
		        m(key) = m.get(key).getOrElse(0.0f) + 1.0f
	        } catch {
		        case e: Exception => {
			        println("Error merging remote map")
			        println("\t"+remoteMap)
			        println("\t"+messages.size)
			        if (messages.size > remoteMap.sinkPart)
				        println("\t"+messages(remoteMap.sinkPart).nodeToCommunity.size)
		        }
	        }
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

  def sparkGraphToUSCGraphs[VD: ClassTag, ED: ClassTag] (graph: SparkGraph[VD, ED],
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

      ei.foreach { edge =>
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
        externalLinks.flatMap { case (source, sinks) =>
          sinks.map { case (vertexId, weight) =>
            val partition = partitionBoundaries.value.find { case (partition, (min, max)) =>
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
