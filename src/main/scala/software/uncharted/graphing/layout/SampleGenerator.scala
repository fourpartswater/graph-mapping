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
package software.uncharted.graphing.layout



import java.io._ //scalastyle:ignore

import scala.util.{Failure, Success, Try}



/**
  * Make a sample dataset for tiling that is easy to debug
  */
object SampleGenerator {
  def writeNode(out: Writer, node: Node): Unit = {
    out.write(node.toString + "\n")
  }

  def writeEdge(out: Writer, edge: Edge): Unit = {
    out.write(edge.toString + "\n")
  }

  private var nextAvailableNodeId: Long = 0L
  def getNextId: Long = {
    val result = nextAvailableNodeId
    nextAvailableNodeId += 1L
    result
  }

  def main (args: Array[String]): Unit = {
    val levels: Int = Try{
      args(0).toInt
    } match {
      case Success(n) => n
      case Failure(e) =>
        println("Required argument: number of levels")
        System.exit(0)
        throw e
    }
    val sampleType = Try{
      args(1) match {
        case "symetric" => 0
        case "asymetric" => 1
        case "registration" => 2
      }
    } match {
      case Success(n) => n
      case Failure(e) => println("Required ")
    }

    val data = sampleType match {
      case 0 => createNodeOctets(levels)
      case 1 => createNodeTriangles(levels)
      case 2 => createRegistrationTest(levels)
    }

    for (level <- 0 to levels) {
      val hierarchyLevel = levels - level
      val directory = new File(s"level_$hierarchyLevel")
      if (!directory.exists()) directory.mkdir()

      val nodeFile = new File(directory, "nodes")
      if (nodeFile.exists()) nodeFile.delete()
      val nodeFileWriter = new OutputStreamWriter(new FileOutputStream(nodeFile))
      data(level)._1.foreach(node => writeNode(nodeFileWriter, node))
      nodeFileWriter.flush()
      nodeFileWriter.close()

      val edgeFile = new File(directory, "edges")
      if (edgeFile.exists()) edgeFile.delete()
      val edgeFileWriter = new OutputStreamWriter(new FileOutputStream(edgeFile))
      data(level)._2.foreach(edge => writeEdge(edgeFileWriter, edge))
      edgeFileWriter.flush()
      edgeFileWriter.close()
    }
  }


  def createRegistrationTest (maxLevel: Int): Array[(Seq[Node], Seq[Edge])] = {
    val nodes = new Array[Seq[Node]](maxLevel + 1)
    val edges = new Array[Seq[Edge]](maxLevel + 1)

    val cx = 128.0
    val cy = 128.0
    for (level <- 0 to maxLevel) {
      val a = 32.0 / (1 << level)
      val b = 16.0 / (1 << level)
      val r = 2.0 / (1 << level)
      val n0 = Node(getNextId, cx - a, cy - b, r, None, 1L, 0, s"$level-0")
      val n1 = Node(getNextId, cx + b, cy - a, r, None, 1L, 0, s"$level-0")
      val n2 = Node(getNextId, cx + a, cy + b, r, None, 1L, 0, s"$level-0")
      val n3 = Node(getNextId, cx - b, cy + a, r, None, 1L, 0, s"$level-0")
      nodes(level) = Seq(n0, n1, n2, n3)
      edges(level) = Seq(
        Edge(n0, n1, false, 1L),
        Edge(n1, n2, false, 1L),
        Edge(n2, n3, false, 1L),
        Edge(n3, n0, false, 1L),
        Edge(n0, n2, false, 2L),
        Edge(n1, n3, false, 2L)
      )
    }

    nodes zip edges
  }

  def createNodeTriangles (maxLevel: Int): Array[(Seq[Node], Seq[Edge])] = {
    val nodes = new Array[Seq[Node]](maxLevel + 1)
    val edges = new Array[Seq[Edge]](maxLevel + 1)

    val n0 = Node(getNextId, 144.0, 112.0, 32.0, None, 1L, 0, "0")
    val n1 = Node(getNextId, 208.0, 176.0, 32.0, None, 1L, 0, "1")
    val n2 = Node(getNextId, 112.0, 240.0, 32.0, None, 1L, 0, "2")
    nodes(0) = Seq(n0, n1, n2)
    edges(0) = Seq(
      Edge(n0, n1, false, 1L), Edge(n1, n2, false, 1L), Edge(n2, n0, false, 1L),
      Edge(n0, n0, false, 1L), Edge(n1, n1, false, 1L), Edge(n2, n2, false, 1L)
    )

    for (level <- 1 to maxLevel) {
      val (nodeSeq, edgeSeq) = createNodeTrianglesAndEdgesForLevel(level, maxLevel, nodes(level - 1))
      nodes(level) = nodeSeq
      edges(level) = edgeSeq
    }

    nodes zip edges
  }

  def createNodeTrianglesAndEdgesForLevel (level: Int, maxLevel: Int, parentLevel: Seq[Node]): (Seq[Node], Seq[Edge]) = {
    val isMaxLevel = (level == maxLevel)

    val (nodes, internalEdges) = parentLevel.map { p =>
      val r = p.radius

      val n0 = Node(getNextId, p.x + r * 0.125, p.y - r * 0.125, r / 4.0, Some(p), 1L, 0, p.metaData + ":0")
      val n1 = Node(getNextId, p.x + r * 0.625, p.y + 4 * 0.375, r / 4.0, Some(p), 1L, 0, p.metaData + ":1")
      val n2 = Node(getNextId, p.x - r * 0.125, p.y + r * 0.875, r / 4.0, Some(p), 1L, 0, p.metaData + ":2")
      (
        Seq(n0, n1, n2),
        Seq(Edge(n0, n1, isMaxLevel, 1L), Edge(n1, n2, isMaxLevel, 1L), Edge(n2, n0, isMaxLevel, 1L))
        )
    }.reduce((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    val maxNumExternalEdges = (1 to level).map(n => 2).reduce(_ * _)
    val r = scala.util.Random
    val externalEdges = (1 to maxNumExternalEdges).flatMap{n =>
      val n1 = nodes(r.nextInt(nodes.length))
      val n2 = nodes(r.nextInt(nodes.length))

      if (n1 != n2 && n1.parent != n2.parent) {
        Some(Edge(n1, n2, isMaxLevel, 1L))
      } else {
        None
      }
    }
    (nodes, internalEdges ++ externalEdges)
  }


  def createNodeOctets (maxLevel: Int): Array[(Seq[Node], Seq[Edge])] = {
    val levels = new Array[Seq[Node]](maxLevel + 1)
    val edges  = new Array[Seq[Edge]](maxLevel + 1)
    levels(0)  = Seq(Node(getNextId, 128.0, 128.0, 128.0, None, 1L, 0, "0"))
    edges(0)   = Seq(Edge(levels(0)(0), levels(0)(0), false, 1L))

    for (level <- 1 to maxLevel) {
      val (lvlSeq, edgeSeq) = createNodeOctetsAndEdgesForLevel(level, maxLevel, levels(level - 1))
      levels(level) = lvlSeq
      edges(level) = edgeSeq
    }

    levels zip edges
  }

  def createNodeOctetsAndEdgesForLevel (level: Int, maxLevel: Int, upperLevel: Seq[Node]): (Seq[Node], Seq[Edge]) = {
    val isMaxLevel = (level == maxLevel)

    def mkEdge (a: Node, b: Node, weight: Long) =
      Edge(a, b, isMaxLevel, weight)


    val (nodes, internalEdges) = upperLevel.map { p /* parent */ =>
      val offset = 256.0 /  (1 to level).map(n => 4.0).reduce(_ * _)

      val node0 = Node(p.id,      p.x,          p.y,          offset/2, Some(p), 1L, 0, p.metaData+":0")
      val node1 = Node(getNextId, p.x - offset, p.y + offset, offset/2, Some(p), 1L, 0, p.metaData+":1")
      val node2 = Node(getNextId, p.x         , p.y + offset, offset/2, Some(p), 1L, 0, p.metaData+":2")
      val node3 = Node(getNextId, p.x + offset, p.y + offset, offset/2, Some(p), 1L, 0, p.metaData+":3")
      val node4 = Node(getNextId, p.x + offset, p.y         , offset/2, Some(p), 1L, 0, p.metaData+":4")
      val node5 = Node(getNextId, p.x + offset, p.y - offset, offset/2, Some(p), 1L, 0, p.metaData+":5")
      val node6 = Node(getNextId, p.x         , p.y - offset, offset/2, Some(p), 1L, 0, p.metaData+":6")
      val node7 = Node(getNextId, p.x - offset, p.y - offset, offset/2, Some(p), 1L, 0, p.metaData+":7")
      val node8 = Node(getNextId, p.x - offset, p.y         , offset/2, Some(p), 1L, 0, p.metaData+":8")

      p.setChildren(node1, node2, node3, node4, node5, node6, node7, node8)
      val nodes = Seq(node0, node1, node2, node3, node4, node5, node6, node7, node8)

      val internalEdges = Seq(
        mkEdge(node0, node1, 1L),
        mkEdge(node0, node2, 1L),
        mkEdge(node0, node3, 1L),
        mkEdge(node0, node4, 1L),
        mkEdge(node0, node5, 1L),
        mkEdge(node0, node6, 1L),
        mkEdge(node0, node7, 1L),
        mkEdge(node0, node8, 1L),
        mkEdge(node1, node4, 1L),
        mkEdge(node4, node7, 1L),
        mkEdge(node7, node2, 1L),
        mkEdge(node2, node5, 1L),
        mkEdge(node5, node8, 1L),
        mkEdge(node8, node3, 1L),
        mkEdge(node3, node6, 1L),
        mkEdge(node6, node1, 1L)
      )
      (nodes, internalEdges)
    }.reduce((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    val maxNumExternalEdges = (1 to level).map(n => 4).reduce(_ * _)
    val r = scala.util.Random
    val externalEdges = (1 to maxNumExternalEdges).flatMap{n =>
      val n1 = nodes(r.nextInt(nodes.length))
      val n2 = nodes(r.nextInt(nodes.length))

      if (n1 != n2 && n1.parent != n2.parent) {
        Some(mkEdge(n1, n2, 1L))
      } else {
        None
      }
    }

    (nodes, (internalEdges ++ externalEdges))
  }

  object Node {
    def apply (id: Long, x: Double, y: Double, radius: Double, parent: Option[Node], numInternalNodes: Long, degree: Int, metaData: String): Node = {
      new Node(id, (x max 0.0) min 255.99999, (y max 0.0) min 255.99999, radius, parent, numInternalNodes, degree, metaData)
    }
  }
  class Node(val id: Long,
             val x: Double,
             val y: Double,
             val radius: Double,
             val parent: Option[Node],
             var numInternalNodes: Long,
             var degree: Int,
             val metaData: String) {
    var children: Option[Seq[Node]] = None

    def setChildren(children: Node*): Unit = {
      addInternalNode(children.length)
      this.children = Some(children)
    }

    def addDegree(n: Int): Unit = {
      degree += n
      parent.map(_.addDegree(n))
    }

    def addInternalNode(n: Long): Unit = {
      numInternalNodes += n
      parent.map(_.addInternalNode(n))
    }

    override def toString: String =
      if (parent.isEmpty) {
        "node\t" + id + "\t" + x + "\t" + y + "\t" + radius + "\t" + id + "\t" + x + "\t" + y + "\t" + radius + "\t" + numInternalNodes + "\t" + degree + "\t" + metaData
      } else {
        val p = parent.get
        "node\t" + id + "\t" + x + "\t" + y + "\t" + radius + "\t" + p.id + "\t" + p.x + "\t" + p.y + "\t" + p.radius + "\t" + numInternalNodes + "\t" + degree + "\t" + metaData
      }

    override def equals(other: Any): Boolean =
      other match {
        case that: Node =>
          this.id == that.id

        case _ => false
      }
  }

  case class Edge(src: Node,
                  dst: Node,
                  maxLevel: Boolean,
                  weight: Long) {
    src.addDegree(1)
    dst.addDegree(1)

    val interCommunityEdge =if ((src.parent != dst.parent) || maxLevel) 1 else 0
    override def toString: String = "edge\t" + src.id + "\t" + src.x + "\t" + src.y + "\t" + dst.id + "\t" + dst.x + "\t" + dst.y + "\t" + weight + "\t" + interCommunityEdge
  }
}
