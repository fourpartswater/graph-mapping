/**
  * This code is copied and translated from https://sites.google.com/site/findcommunities, then modified futher to
  * support analytics and metadata.
  *
  * This means most of it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
  * we can't distribute it without permission - though as a translation, with some optimization for readability in
  * scala, it may be a gray area.
  *
  * TThe rest is:
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

  def withLevel (sc: SparkContext)(level: Int, initialGraph: Graph, initialModularity: Double, community: Community): T = {
    val localVertices = (0 until community.g.nb_nodes).map { n =>
      val id = community.g.id(n)
      val parentId = community.g.nodeInfo(n).communityNode.id
      val size = community.g.internalSize(n)
      val weight = community.g.weighted_degree(n).round.toInt
      val metadata = community.g.metaData(n)
      val analyticData = community.g.nodeInfo(n).finishedAnalyticValues

      val graphNode = new GraphNodeWithAnalytics(id.toLong, parentId.toLong, size, weight, metadata, analyticData)
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

class GraphNodeWithAnalytics (override val id: Long,
                              override val parentId: Long,
                              override val internalNodes: Long,
                              override val degree: Int,
                              override val metadata: String,
                              val analyticData: Seq[String])
  extends GraphNode(id, parentId, internalNodes, degree, metadata) {

  override def toString =
    s"GraphNodeWithAnalytics($id,$parentId,$internalNodes,$degree,$metadata"+analyticData.mkString(",",",",")")
  override def hashCode =
    super.hashCode + analyticData.hashCode() * 7
  override def equals (other: Any): Boolean = other match {
    case that: GraphNodeWithAnalytics =>
      super.equals(that) && this.analyticData == that.analyticData
    case _ => false
  }
}
