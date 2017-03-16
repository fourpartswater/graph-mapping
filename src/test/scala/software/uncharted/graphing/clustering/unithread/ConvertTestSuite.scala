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
package software.uncharted.graphing.clustering.unithread



import java.io.{ByteArrayInputStream, InputStreamReader, BufferedReader}

import org.scalatest.FunSuite



class ConvertTestSuite extends FunSuite {
  test("Test reading of edge files (no weights)") {
    val rawData =
      """edge primary 0 1
        |edge secondary 0 2
        |edge primary 1 3
        |edge secondary 1 4
        |edge primary 2 3
        |edge secondary 2 4""".stripMargin
    val reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(reader, Some("edge"), "[ \t]+", 2, 3, None)
    assert(5 === edges.links.length)
    assert(edges.links(0).toList === List((1, 1.0f, List()), (2, 1.0f, List())))
    assert(edges.links(1).toList === List((0, 1.0f, List()), (3, 1.0f, List()), (4, 1.0f, List())))
    assert(edges.links(2).toList === List((0, 1.0f, List()), (3, 1.0f, List()), (4, 1.0f, List())))
    assert(edges.links(3).toList === List((1, 1.0f, List()), (2, 1.0f, List())))
    assert(edges.links(4).toList === List((1, 1.0f, List()), (2, 1.0f, List())))
  }

  test("Test reading of edge files (with weights)") {
    val rawData =
      """edge primary   0 1 1 0.7
        |edge secondary 0 2 1 0.2
        |edge primary   1 3 1 0.6
        |edge secondary 1 4 1 0.3
        |edge primary   2 3 1 0.5
        |edge secondary 2 4 1 0.4""".stripMargin
    val reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(reader, Some("edge"), "[ \t]+", 2, 3, Some(5))
    assert(5 === edges.links.length)
    assert(edges.links(0).toList === List((1, 0.7f, List()), (2, 0.2f, List())))
    assert(edges.links(1).toList === List((0, 0.7f, List()), (3, 0.6f, List()), (4, 0.3f, List())))
    assert(edges.links(2).toList === List((0, 0.2f, List()), (3, 0.5f, List()), (4, 0.4f, List())))
    assert(edges.links(3).toList === List((1, 0.6f, List()), (2, 0.5f, List())))
    assert(edges.links(4).toList === List((1, 0.3f, List()), (2, 0.4f, List())))
  }

  test("Test adding metadata to edges") {
    val rawData =
      """edge primary 0 1
        |edge secondary 0 2
        |edge primary 1 3
        |edge secondary 1 4
        |edge primary 2 3
        |edge secondary 2 4
        |node 0 2 zero
        |node 1 3 one
        |node 2 3 two
        |node 3 4 three
        |node 4 2 four""".stripMargin
    val edgeReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(edgeReader, Some("edge"), "[ \t]+", 2, 3, None)
    val nodeReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    edges.readMetadata(nodeReader, Some("node"), "[ \t]+", 1, 3, Seq())
    assert(edges.metaData.get.length === 5)
    assert(("zero", List()) === edges.metaData.get.apply(0))
    assert(("one", List()) === edges.metaData.get.apply(1))
    assert(("two", List()) === edges.metaData.get.apply(2))
    assert(("three", List()) === edges.metaData.get.apply(3))
    assert(("four", List()) === edges.metaData.get.apply(4))
  }

  test("Test renumbering of edge files (no weights)") {
    val rawData =
      """edge primary 1 5
        |edge secondary 1 12
        |edge primary 5 12
        |edge secondary 5 23
        |edge primary 12 12
        |edge secondary 12 23""".stripMargin
    val reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(reader, Some("edge"), "[ \t]+", 2, 3, None)
    val newEdges = edges.renumber()
    assert(newEdges.links.length === 4)
    assert(newEdges.links(0).length === 2)
    assert(newEdges.links(0)(0)._1 === 1)
    assert(newEdges.links(2).length === 4)
    assert(newEdges.links(2)(0)._1 === 0)
  }
}
