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
package software.uncharted.graphing



import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random



class RandomGraph (sc: SparkContext) {
  /**
   * Create an RDD of a fixed size
   * @param records The number of records in the RDD
   * @param partitions The number of partitions into which to split these records
   * @return An RDD of the numbers (0 until records)
   */
  def nXm(records: Long, partitions: Int): RDD[Long] = {
    val roughPN = (records / partitions).toDouble.round.toLong
    sc.parallelize(1 to partitions, partitions).mapPartitionsWithIndex { case (p, i) =>
      val start = p * roughPN
      val pn = if (partitions == p + 1) records - start else roughPN
      (0L until pn).map(start + _).iterator
    }
  }

  def makeRandomGraph[T: ClassTag] (nodes: Long, nodePartitions: Int,
                                    edges: Long, edgePartitions: Int,
                                    weightFcn: Random => T): Graph[Long, T] = {
    val nodeData: VertexRDD[Long] = VertexRDD(nXm(nodes, nodePartitions).map(n => (n, n)))
    val edgeData: EdgeRDD[T] = EdgeRDD.fromEdges(nXm(edges, edgePartitions).mapPartitions{i =>
      val R = new Random(System.currentTimeMillis)
      def nextLong (max: Long) = R.nextLong().abs % max
      i.map{n =>
        val source = nextLong(nodes)
        val destination = nextLong(nodes)
        val weight = weightFcn(R)
        Edge[T](source, destination, weight)
      }
    })
    Graph(nodeData, edgeData, -1)
  }
}
