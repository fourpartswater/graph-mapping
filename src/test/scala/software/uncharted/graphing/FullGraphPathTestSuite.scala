/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing


import scala.collection.mutable
import java.io.{File, FileOutputStream}
import java.nio.file.Files

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import org.apache.spark.graphx.{Edge, Graph => SparkGraph}
import software.uncharted.graphing.analytics.{SumAnalytic0, SumAnalytic2}
import software.uncharted.graphing.clustering.unithread.{Graph => InputGraph, _}
import software.uncharted.graphing.layout.forcedirected.{ForceDirectedLayoutParametersParser, LayoutNode}
import software.uncharted.graphing.layout.{Circle, ClusteredGraphLayoutApp, GraphNode, HierarchicFDLayout, HierarchicalLayoutConfig, V2}

import scala.io.Source

/**
  * Test our full graph pipeline, from clustering to layout
  */
class FullGraphPathTestSuite extends FunSuite with SharedSparkContext {
  // Edge analytics - always empty
  private val ea: Seq[String] = Seq()
  private val rawLinks = Array(
    /*  0 */ Seq((1, 1.0f, ea), (2, 1.0f, ea), (3, 1.0f, ea), (4, 1.0f, ea), (6, 1.0f, ea)),
    /*  1 */ Seq((0, 1.0f, ea), (2, 1.0f, ea), (3, 1.0f, ea), (4, 1.0f, ea)),
    /*  2 */ Seq((0, 1.0f, ea), (1, 1.0f, ea), (3, 1.0f, ea), (4, 1.0f, ea)),
    /*  3 */ Seq((0, 1.0f, ea), (1, 1.0f, ea), (2, 1.0f, ea), (4, 1.0f, ea), (14, 1.0f, ea)),
    /*  4 */ Seq((0, 1.0f, ea), (1, 1.0f, ea), (2, 1.0f, ea), (3, 1.0f, ea)),

    /*  5 */ Seq((6, 1.0f, ea), (7, 1.0f, ea), (8, 1.0f, ea), (9, 1.0f, ea)),
    /*  6 */ Seq((5, 1.0f, ea), (7, 1.0f, ea), (8, 1.0f, ea), (9, 1.0f, ea), (11, 1.0f, ea)),
    /*  7 */ Seq((5, 1.0f, ea), (6, 1.0f, ea), (8, 1.0f, ea), (9, 1.0f, ea)),
    /*  8 */ Seq((5, 1.0f, ea), (6, 1.0f, ea), (7, 1.0f, ea), (9, 1.0f, ea), (2, 1.0f, ea)),
    /*  9 */ Seq((0, 1.0f, ea), (6, 1.0f, ea), (7, 1.0f, ea), (8, 1.0f, ea)),

    /* 10 */ Seq((11, 1.0f, ea), (12, 1.0f, ea), (13, 1.0f, ea), (14, 1.0f, ea)),
    /* 11 */ Seq((10, 1.0f, ea), (12, 1.0f, ea), (13, 1.0f, ea), (14, 1.0f, ea)),
    /* 12 */ Seq((10, 1.0f, ea), (11, 1.0f, ea), (13, 1.0f, ea), (14, 1.0f, ea), (3, 1.0f, ea)),
    /* 13 */ Seq((10, 1.0f, ea), (11, 1.0f, ea), (12, 1.0f, ea), (14, 1.0f, ea)),
    /* 14 */ Seq((10, 1.0f, ea), (11, 1.0f, ea), (12, 1.0f, ea), (13, 1.0f, ea), (8, 1.0f, ea)),

    /* 15 */ Seq((16, 1.0f, ea), (17, 1.0f, ea), (18, 1.0f, ea), (19, 1.0f, ea), (21, 1.0f, ea)),
    /* 16 */ Seq((15, 1.0f, ea), (17, 1.0f, ea), (18, 1.0f, ea), (19, 1.0f, ea)),
    /* 17 */ Seq((15, 1.0f, ea), (16, 1.0f, ea), (18, 1.0f, ea), (19, 1.0f, ea)),
    /* 18 */ Seq((15, 1.0f, ea), (16, 1.0f, ea), (17, 1.0f, ea), (19, 1.0f, ea), (29, 1.0f, ea)),
    /* 19 */ Seq((15, 1.0f, ea), (16, 1.0f, ea), (17, 1.0f, ea), (18, 1.0f, ea)),

    /* 20 */ Seq((21, 1.0f, ea), (22, 1.0f, ea), (23, 1.0f, ea), (24, 1.0f, ea)),
    /* 21 */ Seq((20, 1.0f, ea), (22, 1.0f, ea), (23, 1.0f, ea), (24, 1.0f, ea), (26, 1.0f, ea)),
    /* 22 */ Seq((20, 1.0f, ea), (21, 1.0f, ea), (23, 1.0f, ea), (24, 1.0f, ea)),
    /* 23 */ Seq((20, 1.0f, ea), (21, 1.0f, ea), (22, 1.0f, ea), (24, 1.0f, ea), (17, 1.0f, ea)),
    /* 24 */ Seq((20, 1.0f, ea), (21, 1.0f, ea), (22, 1.0f, ea), (23, 1.0f, ea)),

    /* 25 */ Seq((26, 1.0f, ea), (27, 1.0f, ea), (28, 1.0f, ea), (29, 1.0f, ea)),
    /* 26 */ Seq((25, 1.0f, ea), (27, 1.0f, ea), (28, 1.0f, ea), (29, 1.0f, ea)),
    /* 27 */ Seq((25, 1.0f, ea), (26, 1.0f, ea), (28, 1.0f, ea), (29, 1.0f, ea), (18, 1.0f, ea)),
    /* 28 */ Seq((25, 1.0f, ea), (26, 1.0f, ea), (27, 1.0f, ea), (29, 1.0f, ea)),
    /* 29 */ Seq((20, 1.0f, ea), (26, 1.0f, ea), (27, 1.0f, ea), (28, 1.0f, ea), (23, 1.0f, ea))
  )
  private val rawNodes = Array(
    ("Node  0", Seq( "0")), ("Node  1", Seq( "1")), ("Node  2", Seq( "2")), ("Node  3", Seq( "3")), ("Node  4", Seq( "4")),
    ("Node  5", Seq( "5")), ("Node  6", Seq( "6")), ("Node  7", Seq( "7")), ("Node  8", Seq( "8")), ("Node  9", Seq( "9")),
    ("Node 10", Seq("10")), ("Node 11", Seq("11")), ("Node 12", Seq("12")), ("Node 13", Seq("13")), ("Node 14", Seq("14")),
    ("Node 15", Seq("15")), ("Node 16", Seq("16")), ("Node 17", Seq("17")), ("Node 18", Seq("18")), ("Node 19", Seq("19")),
    ("Node 20", Seq("20")), ("Node 21", Seq("21")), ("Node 22", Seq("22")), ("Node 23", Seq("23")), ("Node 24", Seq("24")),
    ("Node 25", Seq("25")), ("Node 26", Seq("26")), ("Node 27", Seq("27")), ("Node 28", Seq("28")), ("Node 29", Seq("29"))
  )

  private def generateData: InputGraph = {
    // Two clusters of 3 clusters of 5 nodes each
    val edgeRepresentation = new GraphEdges(rawLinks)
    edgeRepresentation.metaData = Some(rawNodes)

    edgeRepresentation.toGraph(Array(new SumAnalytic0))
  }

  test("Full graph processing one part at a time") {
    val filesToRemove = mutable.Buffer[File]()
    try {
      // write out our data
      val tmpEdges = File.createTempFile("test-graph", ".edges")
      filesToRemove.append(tmpEdges)
      val edgeOS = new FileOutputStream(tmpEdges)
      rawLinks.zipWithIndex.foreach { case (toNodes, from) =>
        toNodes.foreach { case (to, weight, edgeAnalytics) =>
          edgeOS.write(s"$from\t$to\n".getBytes)
        }
      }
      edgeOS.flush()
      edgeOS.close()

      val tmpNodes = File.createTempFile("test-graph", ".nodes")
      filesToRemove.append(tmpNodes)
      val nodeOS = new FileOutputStream(tmpNodes)
      rawNodes.zipWithIndex.foreach { case ((metaData, analytics), id) =>
        nodeOS.write((s"$id\t$metaData" + analytics.mkString("\t", "\t", "\n")).getBytes)
      }
      nodeOS.flush()
      nodeOS.close()



      // Convert to binary form
      val tmpEdgesBin = File.createTempFile("test-graph", ".edges.bin")
      filesToRemove.append(tmpEdgesBin)
      val tmpNodesBin = File.createTempFile("test-graph", ".nodes.bin")
      filesToRemove.append(tmpNodesBin)

      Convert.main(Array(
        "-ie", tmpEdges.getAbsolutePath, "-ce", "\t", "-s", "0", "-d", "1", "-oe", tmpEdgesBin.getAbsolutePath,
        "-in", tmpNodes.getAbsolutePath, "-cn", "\t", "-n", "0", "-m", "1", "-an", classOf[SumAnalytic2].getName, "-om", tmpNodesBin.getAbsolutePath
      ))



      // Cluster the graph
      val tmpLoc = Files.createTempDirectory("test-graph").toFile
      filesToRemove.append(tmpLoc)
      val clusterLoc = new File(tmpLoc, "clusters")
      clusterLoc.mkdirs()
      Community.main(Array(
        "-m", tmpNodesBin.getAbsolutePath, "-l", "-1", "-n", tmpEdgesBin.getAbsolutePath, "-ac", classOf[SumAnalytic2].getName, "", "-o", clusterLoc.getAbsolutePath
      ))



      // Lay the graph out
      val layoutLoc = new File(tmpLoc, "layout")
      layoutLoc.mkdirs()
      val tmpConfig = File.createTempFile("test-graph", ".config")
      filesToRemove.append(tmpConfig)
      val configOS = new FileOutputStream(tmpConfig)
      configOS.write(
        s"""layout.input.location="${clusterLoc.toURI.toString.dropRight(1)}"
           |layout.input.delimiter="\\t"
           |layout.output.location="${layoutLoc.toURI.toString.dropRight(1)}"
           |layout.max-level=1
           |layout.community-size-threshold=0
           |layout.force-directed.use-node-sizes=true
           |spark.master=local
         """.stripMargin.getBytes)
      configOS.flush()
      configOS.close()
      ClusteredGraphLayoutApp.execute(session, ConfigFactory.parseFile(tmpConfig))

      // Check that our layout produced what it was supposed to
      val (e0, n0) = parseLayoutOutput(new File(layoutLoc, "level_1"))
      val (e1, n1) = parseLayoutOutput(new File(layoutLoc, "level_0"))

      checkOutput(e0, n0, e1, n1)
    } finally {
      filesToRemove.foreach(removeFile)
    }
  }

  // Parse the [file] output of the stand-along layout stage
  private def parseLayoutOutput (location: File): (Seq[Edge[Long]], Map[Long, LayoutNode]) = {
    val contents = location
      .listFiles()
      .filter(_.getName.startsWith("part-"))
      .map(part => Source.fromFile(part).getLines().toArray)
      .reduce(_ ++ _)
    val edges = contents.filter(_.startsWith("edge")).map { line =>
      val fields = line.split("\t")
      new Edge[Long](fields(1).toLong, fields(4).toLong, fields(7).toLong)
    }.toSeq
    val nodes = contents.filter(_.startsWith("node")).map { line =>
      val fields = line.split("\t")
      fields(1).toLong -> new LayoutNode(
        fields(1).toLong, fields(5).toLong, fields(9).toLong, fields(10).toInt, fields.drop(10).mkString("\t"),
        Circle(V2(fields(2).toDouble, fields(3).toDouble), fields(4).toDouble),
        Some(Circle(V2(fields(6).toDouble, fields(7).toDouble), fields(8).toDouble))
      )
    }.toMap
    (edges, nodes)
  }
  private def removeFile (file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(removeFile)
    }
    file.delete
  }

  test("Full graph processing suite") {
    Logger.getRootLogger.setLevel(Level.WARN)
    // Step 1: read in our data, and process it into a graph
    val flatGraph = generateData

    // Step 2: Cluster our graph
    val baseCommunity = new Community(flatGraph, -1, 0.0)
    val clusterer = new CommunityClusterer(baseCommunity, false, false, 0.0, level => new BaselineAlgorithm)
    val clusters = clusterer.doClustering[SparkGraph[GraphNode, Long]](ClusterToLayoutConverter.withLevel(session.sparkContext))

    // Step 3: Do layout
    val layoutConfig = HierarchicalLayoutConfig(null, None, null, null, None,
      256.0, clusters.length - 1, 0)
    val layoutParams =
      ForceDirectedLayoutParametersParser.parse(ConfigFactory.parseString("layout.force-directed.use-node-sizes=true")).get
    def getGraphLevel (level: Int) = clusters(level)
    def withLayout (level: Int, graph: SparkGraph[LayoutNode, Long],
                    width: Double, maxLevel: Boolean) = {
      // Lock this level
      graph.cache()
      graph.vertices.count
      graph.edges.count

      graph
    }
    val arrangedClusters =
      HierarchicFDLayout.determineLayout(
        layoutConfig,
        layoutParams
      )(
        getGraphLevel(_),
        withLayout(_, _, _, _)).map(graph => (graph.vertices.map(_._2).collect, graph.edges.collect)
      )

    assert(2 === arrangedClusters.length)

    checkOutput(
      arrangedClusters(0)._2, arrangedClusters(0)._1.map(node => (node.id, node)).toMap,
      arrangedClusters(1)._2, arrangedClusters(1)._1.map(node => (node.id, node)).toMap
    )
  }

  private def checkOutput (e0: Seq[Edge[Long]], n0: Map[Long, LayoutNode],
                           e1: Seq[Edge[Long]], n1: Map[Long, LayoutNode]): Unit = {

    // Bottom level
    assert(30 === n1.size)
    assert(132 == e1.length || 264 === e1.length)

    // Top level
    assert(6 === n0.size)
    assert(18 === e0.length)

    // Check results in detail
    // Check that nodes are clustered with the nodes with which we expect them to be clustered
    assert(n1(0).parentId === n1(1).parentId)
    assert(n1(0).parentId === n1(2).parentId)
    assert(n1(0).parentId === n1(3).parentId)
    assert(n1(0).parentId === n1(4).parentId)

    assert(n1(5).parentId === n1(6).parentId)
    assert(n1(5).parentId === n1(7).parentId)
    assert(n1(5).parentId === n1(8).parentId)
    assert(n1(5).parentId === n1(9).parentId)

    assert(n1(10).parentId === n1(11).parentId)
    assert(n1(10).parentId === n1(12).parentId)
    assert(n1(10).parentId === n1(13).parentId)
    assert(n1(10).parentId === n1(14).parentId)

    assert(n1(15).parentId === n1(16).parentId)
    assert(n1(15).parentId === n1(17).parentId)
    assert(n1(15).parentId === n1(18).parentId)
    assert(n1(15).parentId === n1(19).parentId)

    assert(n1(20).parentId === n1(21).parentId)
    assert(n1(20).parentId === n1(22).parentId)
    assert(n1(20).parentId === n1(23).parentId)
    assert(n1(20).parentId === n1(24).parentId)

    assert(n1(25).parentId === n1(26).parentId)
    assert(n1(25).parentId === n1(27).parentId)
    assert(n1(25).parentId === n1(28).parentId)
    assert(n1(25).parentId === n1(29).parentId)

    // Check that nodes are not clustered with nodes with which we don't expect them to be clustered
    assert(n1(0).parentId !== n1(5).parentId)
    assert(n1(0).parentId !== n1(10).parentId)
    assert(n1(0).parentId !== n1(15).parentId)
    assert(n1(0).parentId !== n1(20).parentId)
    assert(n1(0).parentId !== n1(25).parentId)

    assert(n1(5).parentId !== n1(10).parentId)
    assert(n1(5).parentId !== n1(15).parentId)
    assert(n1(5).parentId !== n1(20).parentId)
    assert(n1(5).parentId !== n1(25).parentId)

    assert(n1(10).parentId !== n1(15).parentId)
    assert(n1(10).parentId !== n1(20).parentId)
    assert(n1(10).parentId !== n1(25).parentId)

    assert(n1(15).parentId !== n1(20).parentId)
    assert(n1(15).parentId !== n1(25).parentId)

    assert(n1(20).parentId !== n1(25).parentId)


    // Check that clustered nodes are within their parent
    def distance (a: Long, b: Long): Double = {
      (n1(a).geometry.center - n1(b).geometry.center).length
    }
    for (i <- 0 until 30) {
      // Check that parent geometries match
      assert(n1(i).parentGeometry.get.center === n1(n1(i).parentId).geometry.center, s"Node $i's parent isn't centered in it's community")
      assert(n1(i).parentGeometry.get        === n0(n1(i).parentId).geometry,        s"Node $i's parent geometry doesn't match parent's geometry")
      assert(distance(i, n1(i).parentId) < n0(n1(i).parentId).geometry.radius,       s"Node $i mismatch in distance from parent")
    }
  }
}
