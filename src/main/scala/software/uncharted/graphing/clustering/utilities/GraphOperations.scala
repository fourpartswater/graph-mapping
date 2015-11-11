/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.clustering.utilities


import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.graphx._



/**
 * A few standard operations on graphs we will want to reuse a lot.
 */
class GraphOperations[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {
  private def getVertexId[VD] (id: VertexId, data: VD): Long = id
  /**
   * Calculate the modularity of the graph, assuming every node is in its own category
   *
   * This assumes that the graph is undirected - i.e., that each edge is only listed once, but counts twice.
   * @param edgeWeightFcn A function to convert an edge to a numeric weight.  Default is to weigh all edges at value 1.0
   * @return The modularity of the graph as individual, separate nodes
   */
  def calculateIndividualModularity (edgeWeightFcn: ED => Double = edge => 1.0): Double = {
    // Calculate the total degree of the graph
    val m = graph.edges.map(edge => edgeWeightFcn(edge.attr)).reduce(_ + _)
    // Calculate the degree and self-degree of each node
    val graphWithDegrees: Graph[(VD, (Double, Double)), ED] =
      DegreeAndSelfDegreeCalculation.calculateAugmentedVertexInfo(getVertexId[VD], edgeWeightFcn)(graph)

    // Modularity Q is:
    //     Q = 1 / 2 m sum(i, j, (A_i_j - k_i * k_j / 2 m) * delta(c_i, c_j)
    // where delta(c_i, c_j) is 1 if i and j are in the same community, 0 if not
    //
    // Since we are assuming that each vertex is in its own community, this means delta(c_i, c_j) is
    //     delta(c_i, c_j) = if (i == j) 1 else 0
    // so this becomes
    //     Q = 1 / 2 m sum(i, (A_i_i - k_i^2 / 2 m))
    val modularity = graphWithDegrees.vertices.map{case (vertexId, (vertexData, (weight, selfWeight))) =>
        selfWeight - weight * weight * 0.5 / m
    }.reduce(_ + _) * 0.5 / m
    modularity
  }

  /**
   * Calculate the modularity of a graph
   *
   * This assumes the graph is undirected - i.e., that each edge is listed once, but counts twice.
   *
   * Because this calculation requires calculating the relative values between every pair of nodes, connected or not,
   * it requires a cartesian product, and a join, which means it is a slow operation.  Use with care.
   */
  def calculateModularity (edgeWeightFcn: ED => Double = edge => 1.0,
                           nodeCommunityFcn: (VertexId, VD) => Long = (vid, vdata) => vid): Double = {
    // Calculate the total degree of the graph
    val m = graph.edges.map(edge => edgeWeightFcn(edge.attr)).reduce(_ + _)
    // Calculate the degree and intra-community-degree of each node
    val graphWithDegrees: Graph[(VD, Double), ED] =
      DegreeCalculation.calculateAugmentedVertexInfo(getVertexId[VD], edgeWeightFcn)(graph)

    val nodes: RDD[(VertexId, (VD, Double))]= graphWithDegrees.vertices
    val intraCommunityNodePairs =
      (nodes cartesian nodes)
        .filter{case (a, b) =>
          nodeCommunityFcn(a._1, a._2._1) == nodeCommunityFcn(b._1, b._2._1)
        }
        .map{case (a, b) =>
          ((a._1, b._1), (a._2, b._2))
        }
    val edges = graph.edges.flatMap(edge =>
      List(
        ((edge.srcId, edge.dstId), edgeWeightFcn(edge.attr)),
        ((edge.dstId, edge.srcId), edgeWeightFcn(edge.attr))
      )
    ).reduceByKey(_ + _)


    // Modularity Q is:
    //     Q = 1 / 2 m sum(i, j, (A_i_j - k_i * k_j / 2 m) * delta(c_i, c_j)
    // where delta(c_i, c_j) is 1 if i and j are in the same community, 0 if not

    val modularity = intraCommunityNodePairs.leftOuterJoin(edges).map { case ((idI, idJ), (((nodeI, k_i), (nodeJ, k_j)), weightOption)) =>
        weightOption.getOrElse(0.0) - k_i * k_j * 0.5 / m
    }.reduce(_ + _) * 0.5 / m
    modularity
  }
}

object GraphOperations {
  implicit def GraphToOps[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED]): GraphOperations[VD, ED] =
    new GraphOperations(graph)
}


/**
 * Calculate the total degree of each node
 *
 * The passed-in vertex conversion function is ignored.
 * The passed-in edge conversion function should calculate the weight of the edge.
 */
object DegreeCalculation extends EdgeCalculation[Double] {
  type Data = Double
  val dct = implicitly[ClassTag[Data]]
  val defaultData = 0.0
  override val fields = TripletFields.EdgeOnly

  def getEdgeInfo(context: EdgeContext[Long, Double, Data]) = {
    val weight = context.attr
    // Because we assume graphs are undirected, we send the weight to both source and destination
    // This should include self-links (I think)
    Some(Some(weight), Some(weight))
  }

  def mergeEdgeInfo (a: Data, b: Data): Data = a + b
}

/**
 * Calculate the total degree and self-linking degree of every node.
 *
 * The passed-in vertex conversion function is ignored.
 * The passed-in edge conversion function should calculate the weight of the edge.
 */
object DegreeAndSelfDegreeCalculation extends EdgeCalculation[Double] {
  type Data = (Double, Double)
  val dct = implicitly[ClassTag[Data]]
  val defaultData = (0.0, 0.0)
  override val fields = TripletFields.EdgeOnly

  def getEdgeInfo(context: EdgeContext[Long, Double, Data]) = {
    val weight = context.attr
    val selfWeight = if (context.srcId == context.dstId) weight else 0.0
    // Because we assume graphs are undirected, we send the weight to both source and destination
    // This should include self-links (I think)
    Some(Some((weight, selfWeight)), Some((weight, selfWeight)))
  }

  def mergeEdgeInfo (a: Data, b: Data): Data = (a._1 + b._1, a._2 + b._2)
}

/**
 * Calculate the total degree and intra-community degree of every node.
 *
 * The passed-in vertex conversion function should calculate the community of the vertex.
 * The passed-in edge conversion function should calculate the weight of the edge.
 */
object DegreeAndIntraCommunityDegreeCalculation extends EdgeCalculation[Double] {
  type Data = (Double, Double)
  val dct = implicitly[ClassTag[Data]]
  val defaultData = (0.0, 0.0)

  def getEdgeInfo(context: EdgeContext[Long, Double, Data]) = {
    val weight = context.attr
    // Our canonical
    val communityWeight = if (context.srcAttr == context.dstAttr) weight else 0.0

    // Because we assume graphs are undirected, we send the weight to both source and destination
    // This should include self-links (I think)
    Some(Some((weight, communityWeight)), Some((weight, communityWeight)))
  }

  def mergeEdgeInfo (a: Data, b: Data): Data = (a._1 + b._1, a._2 + b._2)
}