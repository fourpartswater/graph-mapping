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

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import software.uncharted.graphing.layout.GraphNode

import scala.collection.Seq

/**
  * This object's sole purpose is to provide a function that converts the output of the clustering process into
  * a form that can be used by the layout process
  */
object ClusterToLayoutConverter {
  type T = org.apache.spark.graphx.Graph[GraphNode, Long]

  def withLevel (sc: SparkContext)(level: Int, initialGraph: Option[Graph], initialModularity: Double, community: Community): T = {
    val localVertices = (0 until community.g.nb_nodes).map { n =>
      val g = community.g
      val id = g.id(n)
      val parentId = g.nodeInfo(n).communityNode.get.id
      val size = g.internalSize(n)
      val weight = g.weightedDegree(n).round.toInt
      val simpleMetadata = g.metaData(n)
      val analyticData = g.nodeInfo(n).finishedAnalyticValues
      val metadataWithAnalytics =
        if (analyticData.length > 0) {
          simpleMetadata + analyticData.mkString("\t", "\t", "")
        } else {
          simpleMetadata
        }

      val graphNode = new GraphNode(id.toLong, parentId.toLong, size, weight, metadataWithAnalytics)
      (id.toLong, graphNode)
    }
    val vertices: RDD[(VertexId, GraphNode)] = sc.parallelize(localVertices)

    val localEdges = (0 until community.g.nb_nodes).flatMap { srcIndex =>
      val src: VertexId = community.g.id(srcIndex)
      community.g.neighbors(srcIndex).map { case (dstIndex, weight) =>
        val dst: VertexId = community.g.id(dstIndex)
          Edge[Long](src, dst, weight.round.toLong)
      }
    }
    val edges: RDD[Edge[Long]] = sc.parallelize(localEdges)

    org.apache.spark.graphx.Graph(vertices, edges)
  }
}
