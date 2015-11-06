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
package software.uncharted.graphing.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, SharedSparkContext}
import org.scalatest.FunSuite
import software.uncharted.graphing.RandomGraph
import software.uncharted.graphing.clustering.experiments.PathClustering
import software.uncharted.graphing.clustering.sotera.{LouvainHarness2, LouvainHarness}

/**
 * Created by nkronenfeld on 10/26/15.
 */
class LouvainProfilingTestSuite extends FunSuite with SharedSparkContext {
  // Time the running of a function, in nanoseconds
  def time[T] (fcn: () => T): (T, Double) = {
    val startTime = System.nanoTime()
    val result = fcn()
    val endTime = System.nanoTime()
    (result, (endTime - startTime)/1000000000.0)
  }

  ignore("Time original louvain clustering") {
    val RG = new RandomGraph(sc)
    val g = RG.makeRandomGraph(1000, 1, 10000, 1, R => (((R.nextLong().abs) % 10)+ 1))
    // Make sure all data is cached, so as to be sure that all timing tests are on an equal footing.
    g.cache
    for (i <- 1 to 10) {
      g.vertices.count
      g.edges.count
    }

    val LH = new LouvainHarness(0.15, 1)
    val LH2 = new LouvainHarness2(0.15, 1)
    val PC = new PathClustering

    val N = 1
    val (oldTimes, newTimes, pathTimes) = (1 to N).map{n =>
      val ot1 = time(() => LH.run(sc, g))._2
      val nt1 = time(() => LH.run(sc, g))._2
      val pt1 = time(() => PC.checkClusters(g))._2
      val pt2 = time(() => PC.checkClusters(g))._2
      val nt2 = time(() => LH.run(sc, g))._2
      val ot2 = time(() => LH.run(sc, g))._2
      (List(ot1, ot2), List(nt1, nt2), List(pt1, pt2))
    }.reduce { (a, b) =>
      (a._1 ++ b._1, a._2 ++ b._2, a._3 ++ b._3)
    }

    val toDrop = if (N > 1) 2 else 1
    val oldTime = oldTimes.drop(toDrop).reduce(_ + _) / oldTimes.drop(toDrop).size
    val newTime = newTimes.drop(toDrop).reduce(_ + _) / newTimes.drop(toDrop).size
    val pathTime = pathTimes.drop(toDrop).reduce(_ + _) / pathTimes.drop(toDrop).size

    println("Ran original clustering on a random graph of 1000 nodes/10000 edges in %f seconds %s".format(oldTime, oldTimes.mkString("[", ", ", "]")))
    println("Ran modified clustering on a random graph of 1000 nodes/10000 edges in %f seconds %s".format(newTime, newTimes.mkString("[", ", ", "]")))
    println("Ran path clustering on a random graph of 1000 nodes/10000 edges in %f seconds %s".format(pathTime, pathTimes.mkString("[", ", ", "]")))
  }
}
