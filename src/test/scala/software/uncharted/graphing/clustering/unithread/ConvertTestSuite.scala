/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.clustering.unithread



import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}

import com.typesafe.config.Config
import org.scalatest.FunSuite
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.salt.core.analytic.Aggregator



class ConvertTestSuite extends FunSuite {
  test("Test reading of edge files (no weights)") {
    val rawData = Iterator(
        "edge primary 0 1",
        "edge secondary 0 2",
        "edge primary 1 3",
        "edge secondary 1 4",
        "edge primary 2 3",
        "edge secondary 2 4"
    )
    val edges = GraphEdges(rawData, Some("edge"), "[ \t]+", 2, 3, None)
    assert(5 === edges.links.length)
    assert(edges.links(0).toList === List((1, 1.0f, List()), (2, 1.0f, List())))
    assert(edges.links(1).toList === List((0, 1.0f, List()), (3, 1.0f, List()), (4, 1.0f, List())))
    assert(edges.links(2).toList === List((0, 1.0f, List()), (3, 1.0f, List()), (4, 1.0f, List())))
    assert(edges.links(3).toList === List((1, 1.0f, List()), (2, 1.0f, List())))
    assert(edges.links(4).toList === List((1, 1.0f, List()), (2, 1.0f, List())))
  }

  test("Test reading of edge files (with weights)") {
    val rawData = Iterator(
        "edge primary   0 1 1 0.7",
        "edge secondary 0 2 1 0.2",
        "edge primary   1 3 1 0.6",
        "edge secondary 1 4 1 0.3",
        "edge primary   2 3 1 0.5",
        "edge secondary 2 4 1 0.4"
    )
    val edges = GraphEdges(rawData, Some("edge"), "[ \t]+", 2, 3, Some(5))
    assert(5 === edges.links.length)
    assert(edges.links(0).toList === List((1, 0.7f, List()), (2, 0.2f, List())))
    assert(edges.links(1).toList === List((0, 0.7f, List()), (3, 0.6f, List()), (4, 0.3f, List())))
    assert(edges.links(2).toList === List((0, 0.2f, List()), (3, 0.5f, List()), (4, 0.4f, List())))
    assert(edges.links(3).toList === List((1, 0.6f, List()), (2, 0.5f, List())))
    assert(edges.links(4).toList === List((1, 0.3f, List()), (2, 0.4f, List())))
  }

  test("Test adding metadata to edges") {
    val rawData = Array(
        "edge primary 0 1",
        "edge secondary 0 2",
        "edge primary 1 3",
        "edge secondary 1 4",
        "edge primary 2 3",
        "edge secondary 2 4",
        "node 0 2 zero",
        "node 1 3 one",
        "node 2 3 two",
        "node 3 4 three",
        "node 4 2 four"
    )
    val edges = GraphEdges(rawData.toIterator, Some("edge"), "[ \t]+", 2, 3, None)
    edges.readMetadata(rawData.toIterator, Some("node"), "[ \t]+", 1, 3, 5, Seq())
    assert(edges.metaData.get.length === 5)
    assert(("zero", List()) === edges.metaData.get.apply(0))
    assert(("one", List()) === edges.metaData.get.apply(1))
    assert(("two", List()) === edges.metaData.get.apply(2))
    assert(("three", List()) === edges.metaData.get.apply(3))
    assert(("four", List()) === edges.metaData.get.apply(4))
  }

  test("Test renumbering of edge files (no weights)") {
    val rawData = Array(
      "edge primary 1 5",
      "edge secondary 1 12",
      "edge primary 5 12",
      "edge secondary 5 23",
      "edge primary 12 12",
      "edge secondary 12 23"
    )
    val edges = GraphEdges(rawData.toIterator, Some("edge"), "[ \t]+", 2, 3, None)
    val newEdges = edges.renumber()
    assert(newEdges.links.length === 4)
    assert(newEdges.links(0).length === 2)
    assert(newEdges.links(0)(0)._1 === 1)
    assert(newEdges.links(2).length === 4)
    assert(newEdges.links(2)(0)._1 === 0)
  }

  test("Test raw reading and conversion to a graph") {
    // The third value in each link is a sequence of link analytics.  Link analytics are currently ignored.
    val links = Array(
      Seq(
        (1, 1.0f, Seq("aaa", "bbb", "ccc")),
        (2, 1.0f, Seq("ddd", "eee", "fff"))
      ),
      Seq(
        (0, 1.0f, Seq("ggg", "hhh", "iii")),
        (1, 1.0f, Seq("jjj", "kkk", "lll"))
      ),
      Seq(
        (0, 1.0f, Seq("mmm", "nnn", "ooo")),
        (1, 1.0f, Seq("ppp", "qqq", "rrr"))
      )
    )
    val edges = new GraphEdges(links)
    val graph = edges.toGraph(Array())

    assert(3 === graph.nb_nodes)
    assert(6 === graph.nb_links)
    assert(2 === graph.nb_neighbors(0))
    assert(2 === graph.nb_neighbors(1))
    assert(2 === graph.nb_neighbors(2))
    assert(Set((1, 1.0f), (2, 1.0f)) === graph.neighbors(0).toSet)
    assert(Set((0, 1.0f), (1, 1.0f)) === graph.neighbors(1).toSet)
    assert(Set((0, 1.0f), (1, 1.0f)) === graph.neighbors(2).toSet)
  }

  test("Test that graph weights are carried over when converting to a graph") {
    val links = Array(
      Seq(
        (0, 0.5f, Seq("aaa", "bbb", "ccc")),
        (1, 1.5f, Seq("ddd", "eee", "fff"))
      ),
      Seq(
        (0, 1.4f, Seq("ggg", "hhh", "iii")),
        (1, 0.3f, Seq("jjj", "kkk", "lll"))
      )
    )
    val edges = new GraphEdges(links)
    val graph = edges.toGraph(Array())

    assert(2 === graph.nb_nodes)
    assert(4 === graph.nb_links)
    assert(2 === graph.nb_neighbors(0))
    assert(2 === graph.nb_neighbors(1))
    assert(Set((0, 0.5f), (1, 1.5f)) === graph.neighbors(0).toSet)
    assert(Set((0, 1.4f), (1, 0.3f)) === graph.neighbors(1).toSet)
  }

  test("Test that node metadata is carried over when converting to a graph") {
    val links = Array(
      Seq(
        (0, 0.5f, Seq("aaa", "bbb", "ccc")),
        (1, 1.5f, Seq("ddd", "eee", "fff"))
      ),
      Seq(
        (0, 1.4f, Seq("ggg", "hhh", "iii")),
        (1, 0.3f, Seq("jjj", "kkk", "lll"))
      )
    )
    val edges = new GraphEdges(links)
    edges.metaData = Some(Array(
      ("node A-F", Seq("a", "f")),
      ("node G-L", Seq("gg", "ll")),
      ("extra node", Seq("1", "6"))
    ))
    val graph = edges.toGraph(Array(TestAnalytic(0), TestAnalytic(1)))

    // Make sure metadata and analytics haven't messed anything up
    assert(2 === graph.nb_nodes)
    assert(4 === graph.nb_links)
    assert(2 === graph.nb_neighbors(0))
    assert(2 === graph.nb_neighbors(1))
    assert(Set((0, 0.5f), (1, 1.5f)) === graph.neighbors(0).toSet)
    assert(Set((0, 1.4f), (1, 0.3f)) === graph.neighbors(1).toSet)
    assert("node A-F" === graph.metaData(0))
    assert("node G-L" === graph.metaData(1))
    assert("a" === graph.nodeInfo(0).analyticData(0))
    assert("f" === graph.nodeInfo(0).analyticData(1))
    assert("gg" === graph.nodeInfo(1).analyticData(0))
    assert("ll" === graph.nodeInfo(1).analyticData(1))
  }
}

case class TestAnalytic (column: Int) extends CustomGraphAnalytic[String] {
  override val name: String = s"Test-${column}"
  override def max(left: String, right: String): String = left
  override def initialize(configs: Config): CustomGraphAnalytic[String] = this
  override def min(left: String, right: String): String = left
  override val aggregator: Aggregator[String, String, String] = new Aggregator[String, String, String] {
    override def default(): String = ""
    override def finish(intermediate: String): String = intermediate
    override def merge(left: String, right: String): String = left+right
    override def add(current: String, next: Option[String]): String = current+next.getOrElse("")
  }
}
