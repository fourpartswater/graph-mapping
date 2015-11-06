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

import org.apache.spark.graphx._

import scala.reflect.ClassTag


/**
 * Isolate information for each vertex in a graph based on its edges
 *
 * @tparam ED The edge type of the input graph
 * @tparam D The data type of the information to be gathered for each vertex
 */
trait EdgeInfoVertexCalculation [ED, D] extends Serializable {
  type Data = D
  val dct: ClassTag[Data]

  def getEdgeInfo[VD](context: EdgeContext[VD, ED, Data]): Unit

  def mergeEdgeInfo(a: Data, b: Data): Data

  def calculateVertexInfo[VD: ClassTag](graph: Graph[VD, ED]): VertexRDD[Data] = {
    val edgeDataFcn: EdgeContext[VD, ED, Data] => Unit = getEdgeInfo[VD](_)
    graph.aggregateMessages(edgeDataFcn, mergeEdgeInfo, TripletFields.EdgeOnly)(dct)
  }
}

/**
 * Augment a graph with additional information calculated from edges, and put into vertices
 *
 * @tparam ED The edge type of the input graph
 * @tparam D The data type of the information to be gathered for each vertex
 * @tparam VDAug The data type of the augmentation information to be added to each vertex
 */
trait EdgeInfoVertexAugmentation[ED, D, VDAug] extends EdgeInfoVertexCalculation[ED, D] {
  type Augmentation = VDAug
  val act: ClassTag[Augmentation]

  def mergeVertexInfo[VD: ClassTag](vid: VertexId, vertexData: VD, edgeDataOption: Option[Data]): (VD, Augmentation)

  def augmentVertexInfo[VD: ClassTag](graph: Graph[VD, ED]): Graph[(VD, Augmentation), ED] = {
    val oct = implicitly[ClassTag[(VD, Augmentation)]]
    graph.outerJoinVertices(calculateVertexInfo[VD](graph))(mergeVertexInfo[VD](_, _, _))(dct, oct)
  }
}


/**
 * Isolate information for each vertex in a graph based on itself, its edges, and its neighbors
 *
 * @tparam VD The vertex type of the input graph
 * @tparam ED The edge type of the input graph
 * @tparam D The data type of the information to be gathered for each vertex
 */
trait NeighborInfoVertexCalculation [VD, ED, D] extends Serializable {
  type Data = D
  val dct : ClassTag[Data]
  val fields = TripletFields.All

  def getEdgeInfo(context: EdgeContext[VD, ED, Data]): Unit
  def mergeEdgeInfo(a: Data, b: Data): Data

  def calculateVertexInfo(graph: Graph[VD, ED]): VertexRDD[Data] = {
    graph.aggregateMessages(getEdgeInfo, mergeEdgeInfo, fields)(dct)
  }
}

trait NeighborInfoVertexTransformation [VD, ED, D, OVD] extends NeighborInfoVertexCalculation[VD, ED, D] {
  type Output = OVD
  val oct: ClassTag[Output]

  def mergeVertexInfo(vid: VertexId, vertexData: VD, edgeDataOption: Option[Data]): Output

  def transformVertexInfo(graph: Graph[VD, ED]): Graph[Output, ED] = {
    graph.outerJoinVertices(calculateVertexInfo(graph))(mergeVertexInfo)(dct, oct)
  }
  def apply (graph: Graph[VD, ED]): Graph[Output, ED] = transformVertexInfo(graph)
}