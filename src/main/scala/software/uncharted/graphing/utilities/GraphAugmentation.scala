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
package software.uncharted.graphing.utilities



import org.apache.spark.graphx._ //scalastyle:ignore

import scala.reflect.ClassTag



/**
 * Augment or modify a graph with additional information calculated from edges and vertices, calculated from canonical
 * forms (so that any graph can be used as input, with conversion functions)
 *
 * @tparam CV The canonical form of the vertices
 * @tparam CE The canonical form of the edges
 */
trait TripletCalculation[CV, CE] extends Serializable {
  /** The type of data to calculate for each vertex */
  type Data
  /** A classtag to the data type */
  val dct: ClassTag[Data]
  /** A default value for vertices with no edges */
  val defaultData: Data
  /** The fields needed for this calculation */
  val fields: TripletFields = TripletFields.All

  /**
   * Given an edge and its vertices, calculate the information to send to the source and destination nodes
   * Information can be sent directly here, using the passed-in context, or can be returned with the expectation that
   * the calculation will handle the transmission.
   *
   * @param context The information about the edge and its vertices
   * @return information to send to the source (first) and destination (second), both optional
   */
  def getEdgeInfo(context: EdgeContext[CV, CE, Data]): Option[(Option[Data], Option[Data])]

  /** sendMsg function for aggregateMessages */
  private def getEdgeInfo[VD, ED] (vertexConversion: (VertexId, VD) => CV, edgeConversion: ED => CE)
                                  (context: EdgeContext[VD, ED, Data]): Unit = {
    getEdgeInfo(new IndirectEdgeContext[VD, CV, ED, CE, Data](vertexConversion, edgeConversion, context))
      .map{case (srcOption, dstOption) =>
        srcOption.map(srcMsg => context.sendToSrc(srcMsg))
        dstOption.map(dstMsg => context.sendToDst(dstMsg))
      }
  }

  /** mergeMsg function for aggregateMessages */
  def mergeEdgeInfo (a: Data, b: Data): Data

  /** Calculate consolidated edge information for each vertex that has it */
  def calculateEdgeInfo[VD, ED] (vertexConversion: (VertexId, VD) => CV, edgeConversion: ED => CE)
                                (graph: Graph[VD, ED]): VertexRDD[Data] = {
    graph.aggregateMessages(getEdgeInfo(vertexConversion, edgeConversion), mergeEdgeInfo, fields)(dct)
  }


  private def augmentVertexInfo[VD] (vertexId: VertexId, vertexData: VD, newVertexDataOption: Option[Data]): (VD, Data) = {
    (vertexData, newVertexDataOption.getOrElse(defaultData))
  }
  /** Merge consolidated edge information back into each vertex */
  def calculateAugmentedVertexInfo[VD: ClassTag, ED] (vertexConversion: (VertexId, VD) => CV, edgeConversion: ED => CE)
                                                     (graph: Graph[VD, ED]): Graph[(VD, Data), ED] = {
    val oct = implicitly[ClassTag[(VD, Data)]]
    graph.outerJoinVertices(calculateEdgeInfo(vertexConversion, edgeConversion)(graph))(augmentVertexInfo)(dct, oct)
  }

  /** Merge consolidated edge information back into each vertex, and convert to a new form */
  def calculateModifiedVertexInfo[VDI: ClassTag, VDO: ClassTag, ED] (vertexConversion: (VertexId, VDI) => CV,
                                                                     edgeConversion: ED => CE,
                                                                     calculateResult: (VertexId, VDI, Option[Data]) => VDO)
                                                                    (graph: Graph[VDI, ED]): Graph[VDO, ED] = {
    val oct = implicitly[ClassTag[VDO]]
    graph.outerJoinVertices(calculateEdgeInfo(vertexConversion, edgeConversion)(graph))(calculateResult)(dct, oct)
  }
}

/**
 * Augment or modify a graph with additional information calculated from edges only, calculated from a canonical
 * form (so that any graph can be used as input, with a conversion function)
 *
 * This just uses TripletCalculation, with the vertex ID as the canonical vertex form; TripletForm methods are
 * accessible, but we provide some simpler forms with a default vertex conversion function
 *
 * @tparam CE The canonical form of the edges
 */
trait EdgeCalculation[CE] extends TripletCalculation[Long, CE] {
  override val fields = TripletFields.EdgeOnly

  def convertVertex[VD] (vertexId: VertexId, vertexData: VD): Long = vertexId

  /** Calculate consolidated edge information for each vertex that has it */
  def calculateEdgeInfo[VD, ED] (edgeConversion: ED => CE)
                                (graph: Graph[VD, ED]): VertexRDD[Data] = {
    calculateEdgeInfo(convertVertex[VD], edgeConversion)(graph)
  }


  /** Merge consolidated edge information back into each vertex */
  def calculateAugmentedVertexInfo[VD: ClassTag, ED] (edgeConversion: ED => CE)
                                                     (graph: Graph[VD, ED]): Graph[(VD, Data), ED] = {
    calculateAugmentedVertexInfo(convertVertex[VD], edgeConversion)(graph)
  }

  /** Merge consolidated edge information back into each vertex, and convert to a new form */
  def calculateModifiedVertexInfo[VDI: ClassTag, VDO: ClassTag, ED] (edgeConversion: ED => CE,
                                                                     calculateResult: (VertexId, VDI, Option[Data]) => VDO)
                                                                    (graph: Graph[VDI, ED]): Graph[VDO, ED] = {
    calculateModifiedVertexInfo(convertVertex[VDI], edgeConversion, calculateResult)(graph)
  }
}

/**
 * Augment or modify a graph with additional information calculated from vertices only, calculated from a canonical
 * form (so that any graph can be used as input, with a conversion function)
 *
 * This just uses TripletCalculation, with 1.0 as the canonical edge form; TripletForm methods are accessible, but we
 * provide some simpler forms with a default vertex conversion function
 *
 * @tparam CV The canonical form of the vertices
 */
trait VertexCalculation[CV] extends TripletCalculation[CV, Double] {
  def convertEdge[ED] (edge: ED): Double = 1.0

  /** Calculate consolidated edge information for each vertex that has it */
  def calculateEdgeInfo[VD, ED] (vertexConversion: (VertexId, VD) => CV)
                                (graph: Graph[VD, ED]): VertexRDD[Data] = {
    calculateEdgeInfo(vertexConversion, convertEdge[ED])(graph)
  }


  /** Merge consolidated edge information back into each vertex */
  def calculateAugmentedVertexInfo[VD: ClassTag, ED] (vertexConversion: (VertexId, VD) => CV)
                                                     (graph: Graph[VD, ED]): Graph[(VD, Data), ED] = {
    calculateAugmentedVertexInfo(vertexConversion, convertEdge[ED])(graph)
  }

  /** Merge consolidated edge information back into each vertex, and convert to a new form */
  def calculateModifiedVertexInfo[VDI: ClassTag, VDO: ClassTag, ED] (vertexConversion: (VertexId, VDI) => CV,
                                                                     calculateResult: (VertexId, VDI, Option[Data]) => VDO)
                                                                    (graph: Graph[VDI, ED]): Graph[VDO, ED] = {
    calculateModifiedVertexInfo(vertexConversion, convertEdge[ED], calculateResult)(graph)
  }
}



class IndirectEdgeContext[VD, CV, ED, CE, Data] (vertexConversion: (VertexId, VD) => CV,
                                                 edgeConversion: ED => CE,
                                                 sourceContext: EdgeContext[VD, ED, Data]) extends EdgeContext[CV, CE, Data] {
  override def srcId: VertexId = sourceContext.srcId
  override def srcAttr: CV = vertexConversion(sourceContext.srcId, sourceContext.srcAttr)
  override def sendToSrc(msg: Data): Unit = sourceContext.sendToSrc(msg)

  override def dstId: VertexId = sourceContext.dstId
  override def dstAttr: CV = vertexConversion(sourceContext.dstId, sourceContext.dstAttr)
  override def sendToDst(msg: Data): Unit = sourceContext.sendToDst(msg)

  override def attr: CE = edgeConversion(sourceContext.attr)
}
