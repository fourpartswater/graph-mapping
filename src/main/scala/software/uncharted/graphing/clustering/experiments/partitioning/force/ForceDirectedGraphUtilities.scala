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
package software.uncharted.graphing.clustering.experiments.partitioning.force

import org.apache.spark.graphx._ //scalastyle:ignore
import software.uncharted.graphing.utilities.{EdgeCalculation, TripletCalculation, VertexCalculation}

import scala.reflect.ClassTag


/**
  * Created by nkronenfeld on 2016-01-16.
  */
object ForceDirectedGraphUtilities {
  def addForceDirectedInfo[VD: ClassTag, ED: ClassTag] (g: Graph[VD, ED], dimensions: Int,
                                                        weightFcn: ED => Double):
  Graph[(VD, Vector, Double), ED] = {
    val WandP = new WeightsAndPositions(dimensions)
    WandP.calculate(g, weightFcn).cache()
  }


  def forceDirectedMotion[VD, ED] (g: Graph[(VD, Vector, Double), ED],
                                   dimensions: Int,
                                   weightFcn: ED => Double):
  Graph[(VD, Vector, Double), ED] = {
    val motionEngine = new VertexMotionGenerator(dimensions)

    val center = CenterOfMassCalculator.calculate(g, weightFcn)
    motionEngine.moveVertices(g, center, weightFcn)
  }
}

class WeightsAndPositions (dimension: Int = 2) extends EdgeCalculation[Double] {
  type Data = Double
  val dct = implicitly[ClassTag[Data]]
  val defaultData = 0.0

  /**
    * Given an edge and its vertices, calculate the information to send to the source and destination nodes
    * Information can be sent directly here, using the passed-in context, or can be returned with the expectation that
    * the calculation will handle the transmission.
    *
    * @param context The information about the edge and its vertices
    * @return information to send to the source (first) and destination (second), both optional
    */
  override def getEdgeInfo(context: EdgeContext[VertexId, Double, Double]): Option[(Option[Double], Option[Double])] = {
    Some(Some(context.attr), Some(context.attr))
  }

  /** mergeMsg function for aggregateMessages */
  override def mergeEdgeInfo(a: Double, b: Double): Double = a + b

  def calculate[VD: ClassTag, ED: ClassTag] (g: Graph[VD, ED],
                                             edgeWeightFcn: ED => Double): Graph[(VD, Vector, Double), ED] = {
    val calculateResult: (VertexId, VD, Option[Double]) => (VD, Vector, Double) = (id, data, weight) => {
      (data, Vector.randomVector(dimension), weight.getOrElse(0.0))
    }
    calculateModifiedVertexInfo(edgeWeightFcn, calculateResult)(g)
  }
}

object CenterOfMassCalculator {
  def calculate[VD, ED] (g: Graph[(VD, Vector, Double), ED], fcn: ED => Double):
  Vector = {
    // Calculate the center of mass of the set of vertices, with the weight of the vertex
    // standing in for the mass
    val (totalCoords, totalWeight) =
      g.vertices.map{case (id, (data, coordinates, weight)) =>
        (coordinates * weight, weight)
      }.reduce{(a, b) =>
        (a._1 + b._1, a._2 + b._2)
      }

    totalCoords / totalWeight
  }
}

// Takes (position, weight) and edge weight, and moves positions
class VertexMotionGenerator (dimension: Int = 2) extends TripletCalculation[(Vector, Double), Double] {
  type Data = Vector
  val dct = implicitly[ClassTag[Data]]
  val defaultData = Vector.zeroVector(dimension)

  /**
    * Given an edge and its vertices, calculate the information to send to the source and destination nodes
    * Information can be sent directly here, using the passed-in context, or can be returned with the expectation that
    * the calculation will handle the transmission.
    *
    * @param context The information about the edge and its vertices
    * @return information to send to the source (first) and destination (second), both optional
    */
  override def getEdgeInfo(context: EdgeContext[(Vector, Double), Double, Vector]):
  Option[(Option[Vector], Option[Vector])] = {
    val (srcPos, srcWeight) = context.srcAttr
    val (dstPos, dstWeight) = context.dstAttr
    val edgeWeight = context.attr
    val srcToDst = srcPos to dstPos
    val len = srcToDst.length

    Some((
      Some(srcToDst * (len * edgeWeight / srcWeight)),
      Some(-srcToDst * (len * edgeWeight / dstWeight))
      ))
  }

  /** mergeMsg function for aggregateMessages */
  override def mergeEdgeInfo(a: Vector, b: Vector): Vector = a + b

  def moveVertices[VD, ED](g: Graph[(VD, Vector, Double), ED],
                           center: Vector,
                           edgeWeightFcn: ED => Double): Graph[(VD, Vector, Double), ED] = {
    val sc = g.vertices.context

    val centralForce = sc.doubleAccumulator("centralForce")
    val pointForce = sc.doubleAccumulator("pointForce")

    val extractVertexInfo: (VertexId, (VD, Vector, Double)) => (Vector, Double) =
      (id, data) => (data._2, data._3)
    val calculateResult: (VertexId, (VD, Vector, Double), Option[Vector]) => (VD, Vector, Double) =
      (id, vertexInfo, graphForceOpt) => {
        val (data, position, weight) = vertexInfo

        val centerToVertex = center to position
        // make center force inversely proportional to distance
        val centerForce = centerToVertex / (centerToVertex.lengthSquared * 10.0) + (center to Vector.constantVector(center.dimensions, 0.5))

        val totalForce = graphForceOpt.map(graphForce => (graphForce * 10) + centerForce).getOrElse(centerForce)

        centralForce.add(centerForce.length)
        graphForceOpt.foreach(gf => pointForce.add(gf.length))

        // Use a fraction of total force, so movement is slow
        (data, position + totalForce/20.0, weight)
      }

    val result = calculateModifiedVertexInfo[(VD, Vector, Double), (VD, Vector, Double), ED](
      extractVertexInfo,
      edgeWeightFcn,
      calculateResult
    )(g)
    result.cache()
    result.vertices.count()

    result
  }
}
