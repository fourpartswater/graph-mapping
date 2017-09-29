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
package software.uncharted.graphing.clustering.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.test.SharedSQLContext
import org.scalactic.TolerantNumerics

class KMeansClustererTestSuite extends SharedSQLContext {
  Logger.getRootLogger().setLevel(Level.WARN)
  import testImplicits._
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1E-12)

  private val smallSet = Seq(
    Vectors.dense( 1.0,  0.0), Vectors.dense( 0.0,  1.0),
    Vectors.dense( 9.0,  0.0), Vectors.dense(10.0,  1.0),
    Vectors.dense(10.0,  9.0), Vectors.dense( 9.0, 10.0),
    Vectors.dense( 1.0, 10.0), Vectors.dense( 0.0,  9.0)
  ).zipWithIndex

  private val largeBalancedSet = Seq(
    Vectors.dense(0.51, 0.51), Vectors.dense(0.51, 0.52), Vectors.dense(0.52, 0.51), Vectors.dense(0.52, 0.52),
    Vectors.dense(0.51, 1.51), Vectors.dense(0.51, 1.52), Vectors.dense(0.52, 1.51), Vectors.dense(0.52, 1.52),
    Vectors.dense(1.51, 0.51), Vectors.dense(1.51, 0.52), Vectors.dense(1.52, 0.51), Vectors.dense(1.52, 0.52),
    Vectors.dense(1.51, 1.51), Vectors.dense(1.51, 1.52), Vectors.dense(1.52, 1.51), Vectors.dense(1.52, 1.52),

    Vectors.dense(10.51, 0.51), Vectors.dense(10.51, 0.52), Vectors.dense(10.52, 0.51), Vectors.dense(10.52, 0.52),
    Vectors.dense(10.51, 1.51), Vectors.dense(10.51, 1.52), Vectors.dense(10.52, 1.51), Vectors.dense(10.52, 1.52),
    Vectors.dense(11.51, 0.51), Vectors.dense(11.51, 0.52), Vectors.dense(11.52, 0.51), Vectors.dense(11.52, 0.52),
    Vectors.dense(11.51, 1.51), Vectors.dense(11.51, 1.52), Vectors.dense(11.52, 1.51), Vectors.dense(11.52, 1.52),

    Vectors.dense(0.51, 10.51), Vectors.dense(0.51, 10.52), Vectors.dense(0.52, 10.51), Vectors.dense(0.52, 10.52),
    Vectors.dense(0.51, 11.51), Vectors.dense(0.51, 11.52), Vectors.dense(0.52, 11.51), Vectors.dense(0.52, 11.52),
    Vectors.dense(1.51, 10.51), Vectors.dense(1.51, 10.52), Vectors.dense(1.52, 10.51), Vectors.dense(1.52, 10.52),
    Vectors.dense(1.51, 11.51), Vectors.dense(1.51, 11.52), Vectors.dense(1.52, 11.51), Vectors.dense(1.52, 11.52),

    Vectors.dense(10.51, 10.51), Vectors.dense(10.51, 10.52), Vectors.dense(10.52, 10.51), Vectors.dense(10.52, 10.52),
    Vectors.dense(10.51, 11.51), Vectors.dense(10.51, 11.52), Vectors.dense(10.52, 11.51), Vectors.dense(10.52, 11.52),
    Vectors.dense(11.51, 10.51), Vectors.dense(11.51, 10.52), Vectors.dense(11.52, 10.51), Vectors.dense(11.52, 10.52),
    Vectors.dense(11.51, 11.51), Vectors.dense(11.51, 11.52), Vectors.dense(11.52, 11.51), Vectors.dense(11.52, 11.52)
  ).zipWithIndex

  test("Basic cluster in local K-Means clustering") {
    val data = smallSet
    val output = KMeansClusterer.clusterSetLocally[(Vector, Int)](Seq(4), _._1, seed = 0)(data)
    val clusterMap = output.map { case ((v, i), c, d) => (i, c) }.toMap

    assert(clusterMap(0) === clusterMap(1))
    assert(clusterMap(2) === clusterMap(3))
    assert(clusterMap(4) === clusterMap(5))
    assert(clusterMap(6) === clusterMap(7))
    assert(clusterMap(0) !== clusterMap(2))
    assert(clusterMap(0) !== clusterMap(4))
    assert(clusterMap(0) !== clusterMap(6))
    assert(clusterMap(2) !== clusterMap(4))
    assert(clusterMap(2) !== clusterMap(6))
    assert(clusterMap(4) !== clusterMap(6))
  }

  test("Basic clustering in distributed K-means clustering") {
    Logger.getRootLogger().setLevel(Level.WARN)
    val data = smallSet.toDF("value", "id")

    val output = KMeansClusterer.clusterSetDistributed(Seq(4), "value", "result", seed = 0)(data)
    val clusterMap = output.select(output("id").as[Int], output("result").as[Int]).collect.toMap

    assert(clusterMap(0) === clusterMap(1))
    assert(clusterMap(2) === clusterMap(3))
    assert(clusterMap(4) === clusterMap(5))
    assert(clusterMap(6) === clusterMap(7))
    assert(clusterMap(0) !== clusterMap(2))
    assert(clusterMap(0) !== clusterMap(4))
    assert(clusterMap(0) !== clusterMap(6))
    assert(clusterMap(2) !== clusterMap(4))
    assert(clusterMap(2) !== clusterMap(6))
    assert(clusterMap(4) !== clusterMap(6))
  }

  test("Choice of K in local K-Means clustering") {
    val data = smallSet
    for (i <- 4 to 10) {
      val output = KMeansClusterer.clusterSetLocally[(Vector, Int)](2 to i, _._1, seed = 0)(data)
      val clusterMap = output.map { case ((v, i), c, d) => (i, c) }.toMap

      assert(clusterMap.values.toSet.size === 4, s"Not 4 sets when i = $i")
    }
  }

  test("Choice of K in distributed K-Means clustering") {
    Logger.getRootLogger().setLevel(Level.WARN)
    val data = smallSet.toDF("value", "id").cache()

    for (i <- 4 to 10) {
      val output = KMeansClusterer.clusterSetDistributed(2 to i, "value", "result", seed = 0)(data)
      val clusterMap = output.select(output("id").as[Int], output("result").as[Int]).collect.toMap

      assert(clusterMap.values.toSet.size === 4, s"Not 4 sets when i = $i")
    }
  }

  test("Choice of K in local K-Means clustering, when the best choice is the minimum possible choice") {
    val data = smallSet
    val output = KMeansClusterer.clusterSetLocally[(Vector, Int)](4 to 6, _._1, seed = 0)(data)
    val clusterMap = output.map { case ((v, i), c, d) => (i, c) }.toMap

    // Make sure there are 4 clusters
    assert(clusterMap.values.toSet.size === 4)
  }

  test("Distance from cluster center in local K-Means clustering") {
    val data = smallSet
    val output = KMeansClusterer.clusterSetLocally[(Vector, Int)](Seq(4), _._1, seed = 0)(data)
    val clusterMap = output.map { case ((v, i), c, d) => (i, (c, d)) }.toMap

    // The right elements should be in each cluster
    assert(clusterMap(0)._1 === clusterMap(1)._1)
    assert(clusterMap(2)._1 === clusterMap(3)._1)
    assert(clusterMap(4)._1 === clusterMap(5)._1)
    assert(clusterMap(6)._1 === clusterMap(7)._1)

    // two-element clusters - both elements should be the same distance to the cluster center
    assert(clusterMap(0)._2 === clusterMap(1)._2)
    assert(clusterMap(2)._2 === clusterMap(3)._2)
    assert(clusterMap(4)._2 === clusterMap(5)._2)
    assert(clusterMap(6)._2 === clusterMap(7)._2)

    // Clusters should be distinct
    assert(clusterMap(0)._1 !== clusterMap(2)._1)
    assert(clusterMap(0)._1 !== clusterMap(4)._1)
    assert(clusterMap(0)._1 !== clusterMap(6)._1)
    assert(clusterMap(2)._1 !== clusterMap(4)._1)
    assert(clusterMap(2)._1 !== clusterMap(6)._1)
    assert(clusterMap(4)._1 !== clusterMap(6)._1)
  }

  test("Distance from cluster center in distributed K-Means clustering") {
    Logger.getRootLogger().setLevel(Level.WARN)
    val data = smallSet.toDF("value", "id").cache()
    val output = KMeansClusterer.clusterSetDistributed(Seq(4), "value", "result", Some("distance"), seed = 0)(data)
    val clusterMap = output.select(output("id").as[Int], output("result").as[Int], output("distance").as[Double])
      .collect()
      .map { case (id, cluster, distance) => (id, (cluster, distance)) }
      .toMap

    // The right elements should be in each cluster
    assert(clusterMap(0)._1 === clusterMap(1)._1)
    assert(clusterMap(2)._1 === clusterMap(3)._1)
    assert(clusterMap(4)._1 === clusterMap(5)._1)
    assert(clusterMap(6)._1 === clusterMap(7)._1)

    // two-element clusters - both elements should be the same distance to the cluster center
    assert(clusterMap(0)._2 === clusterMap(1)._2)
    assert(clusterMap(2)._2 === clusterMap(3)._2)
    assert(clusterMap(4)._2 === clusterMap(5)._2)
    assert(clusterMap(6)._2 === clusterMap(7)._2)

    // Clusters should be distinct
    assert(clusterMap(0)._1 !== clusterMap(2)._1)
    assert(clusterMap(0)._1 !== clusterMap(4)._1)
    assert(clusterMap(0)._1 !== clusterMap(6)._1)
    assert(clusterMap(2)._1 !== clusterMap(4)._1)
    assert(clusterMap(2)._1 !== clusterMap(6)._1)
    assert(clusterMap(4)._1 !== clusterMap(6)._1)

    clusterMap.values.foreach { case (cluster, distance) => assert(0.0 !== distance) }
  }

  test("Heirarchical clustering of a simple dataset using local K-Means clustering") {
    val data = largeBalancedSet

    val output = KMeansClusterer.heirarchicalClusterSetLocally[(Vector, Int)](2 to 5, p => p._1, p => p._2, seed = 0)(data)
    val clusterMap = output.map { case ((feature, id), cluster) => (id, cluster) }.toMap

    // Check that everything is clustered 2 levels deep
    for (i <- 0 until 64) {
      assert(3 === clusterMap(i).length)
    }

    // Check that there are 4 top-level blocks of 16, each containing 4 second-level blocks of 4
    for (i0 <- 0 until 4) {
      val base0 = clusterMap(i0 * 16)(2)
      for (i1 <- 0 until 16) {
        assert(clusterMap(i0 * 16 + i1)(2) === base0, s"i0 = $i0, i1 = $i1")
      }

      for (i1 <- 0 until 4) {
        val base1 = clusterMap(i0 * 16 + i1 * 4)(1)
        for (i2 <- 0 until 4) {
          assert(base1 === clusterMap(i0 * 16 + i1 * 4 + i2)(1), s"i0 = $i0, i1 = $i1, i2 = $i2")
        }
      }
    }
  }

  test("Heirarchical clustering of a simple dataset using distributed K-Means clustering") {
    Logger.getRootLogger().setLevel(Level.WARN)
    val data = largeBalancedSet.toDF("value", "id").cache

    val output = KMeansClusterer.heirarchicalClusterSetDistributed(2 to 5, "id", "value", "cluster", seed = 0)(data)
    val clusterMap = output.select(output("id").as[Int], output("cluster").as[Seq[Int]])
      .collect.toMap

    // Check that everything is clustered 2 levels deep
    for (i <- 0 until 64) {
      assert(3 === clusterMap(i).length)
    }

    // Check that there are 4 top-level blocks of 16, each containing 4 second-level blocks of 4
    for (i0 <- 0 until 4) {
      val base0 = clusterMap(i0 * 16)(2)
      for (i1 <- 0 until 16) {
        assert(clusterMap(i0 * 16 + i1)(2) === base0, s"i0 = $i0, i1 = $i1")
      }

      for (i1 <- 0 until 4) {
        val base1 = clusterMap(i0 * 16 + i1 * 4)(1)
        for (i2 <- 0 until 4) {
          assert(base1 === clusterMap(i0 * 16 + i1 * 4 + i2)(1), s"i0 = $i0, i1 = $i1, i2 = $i2")
        }
      }
    }
  }

  test("Use of the central point of a cluster to ID the cluster in distributed K-Means clustering") {
    Logger.getRootLogger().setLevel(Level.WARN)
    val data = Seq(
      Vectors.dense(  0.0,  4.0), Vectors.dense(  0.0,  5.0), Vectors.dense(  0.0,  6.0),  // 0, 1, 2
      Vectors.dense(  0.0, 14.0), Vectors.dense(  0.0, 15.0), Vectors.dense(  0.0, 16.0),  // 3, 4, 5
      Vectors.dense(  0.0, 24.0), Vectors.dense(  0.0, 25.0), Vectors.dense(  0.0, 26.0),  // 6, 7, 8
      Vectors.dense(100.0,  4.0), Vectors.dense(100.0,  5.0), Vectors.dense(100.0,  6.0),  // 9, 10, 11
      Vectors.dense(100.0, 14.0), Vectors.dense(100.0, 15.0), Vectors.dense(100.0, 16.0),  // 12, 13, 14
      Vectors.dense(100.0, 24.0), Vectors.dense(100.0, 25.0), Vectors.dense(100.0, 26.0),  // 15, 16, 17
      Vectors.dense(200.0,  4.0), Vectors.dense(200.0,  5.0), Vectors.dense(200.0,  6.0),  // 18, 19, 20
      Vectors.dense(200.0, 14.0), Vectors.dense(200.0, 15.0), Vectors.dense(200.0, 16.0),  // 21, 22, 23
      Vectors.dense(200.0, 24.0), Vectors.dense(200.0, 25.0), Vectors.dense(200.0, 26.0)   // 24, 25, 26
    ).zipWithIndex.toDF("features", "id")

    val output =
      KMeansClusterer.heirarchicalClusterSetDistributed(Seq(3), "id", "features", "cluster", seed = 0)(data)
    val clusterMap = output.select(output("id").as[Int], output("cluster").as[Array[Int]])
      .collect().toMap.mapValues(_.toList)

    assert(clusterMap( 0) === List( 0,  1,  4))
    assert(clusterMap( 1) === List( 1,  1,  4))
    assert(clusterMap( 2) === List( 2,  1,  4))
    assert(clusterMap( 3) === List( 3,  4,  4))
    assert(clusterMap( 4) === List( 4,  4,  4))
    assert(clusterMap( 5) === List( 5,  4,  4))
    assert(clusterMap( 6) === List( 6,  7,  4))
    assert(clusterMap( 7) === List( 7,  7,  4))
    assert(clusterMap( 8) === List( 8,  7,  4))
    assert(clusterMap( 9) === List( 9, 10, 13))
    assert(clusterMap(10) === List(10, 10, 13))
    assert(clusterMap(11) === List(11, 10, 13))
    assert(clusterMap(12) === List(12, 13, 13))
    assert(clusterMap(13) === List(13, 13, 13))
    assert(clusterMap(14) === List(14, 13, 13))
    assert(clusterMap(15) === List(15, 16, 13))
    assert(clusterMap(16) === List(16, 16, 13))
    assert(clusterMap(17) === List(17, 16, 13))
    assert(clusterMap(18) === List(18, 19, 22))
    assert(clusterMap(19) === List(19, 19, 22))
    assert(clusterMap(20) === List(20, 19, 22))
    assert(clusterMap(21) === List(21, 22, 22))
    assert(clusterMap(22) === List(22, 22, 22))
    assert(clusterMap(23) === List(23, 22, 22))
    assert(clusterMap(24) === List(24, 25, 22))
    assert(clusterMap(25) === List(25, 25, 22))
    assert(clusterMap(26) === List(26, 25, 22))
  }

  test("Padding of clusters to an even depth in distributed K-Means clustering") {
    Logger.getRootLogger().setLevel(Level.WARN)
    val data = Seq(
      Vectors.dense(  0.0,   0.0), Vectors.dense(  1.0,   0.0), Vectors.dense(  0.0,   1.0), Vectors.dense( -1.0,   0.0), Vectors.dense(  0.0,  -1.0),
      Vectors.dense( 10.0,   0.0), Vectors.dense( 11.0,   0.0), Vectors.dense( 10.0,   1.0), Vectors.dense(  9.0,   0.0), Vectors.dense( 10.0,  -1.0),
      Vectors.dense(  0.0,  10.0), Vectors.dense(  1.0,  10.0), Vectors.dense(  0.0,  11.0), Vectors.dense( -1.0,  10.0), Vectors.dense(  0.0,   9.0),
      Vectors.dense(-10.0,   0.0), Vectors.dense( -9.0,   0.0), Vectors.dense(-10.0,   1.0), Vectors.dense(-11.0,   0.0), Vectors.dense(-10.0,  -1.0),
      Vectors.dense(  0.0, -10.0), Vectors.dense(  1.0, -10.0), Vectors.dense(  0.0,  -9.0), Vectors.dense( -1.0, -10.0), Vectors.dense(  0.0, -11.0),
      Vectors.dense( 50.0,   0.0), Vectors.dense( 51.0,   0.0), Vectors.dense( 50.0,   1.0), Vectors.dense( 49.0,   0.0), Vectors.dense( 50.0,  -1.0),
      Vectors.dense(  0.0,  50.0), Vectors.dense(  0.0,  51.0), Vectors.dense(  1.0,  50.0), Vectors.dense(  0.0,  49.0), Vectors.dense( -1.0,  50.0),
      Vectors.dense(-50.0,   0.0), Vectors.dense(-49.0,   0.0), Vectors.dense(-50.0,   1.0), Vectors.dense(-51.0,   0.0), Vectors.dense(-50.0,  -1.0),
      Vectors.dense(  0.0, -50.0), Vectors.dense(  1.0, -50.0), Vectors.dense(  0.0, -49.0), Vectors.dense( -1.0, -50.0), Vectors.dense(  0.0, -51.0)
    ).zipWithIndex.toDF("features", "id")

    val output =
      KMeansClusterer.heirarchicalClusterSetDistributed(Seq(5), "id", "features", "cluster", seed = 0)(data)
    val clusterMap = output.select(output("id").as[Int], output("cluster").as[Array[Int]])
      .collect().toMap.mapValues(_.toList)

    assert(clusterMap( 0) === List( 0,  0,  0))
    assert(clusterMap( 1) === List( 1,  0,  0))
    assert(clusterMap( 2) === List( 2,  0,  0))
    assert(clusterMap( 3) === List( 3,  0,  0))
    assert(clusterMap( 4) === List( 4,  0,  0))
    assert(clusterMap( 5) === List( 5,  5,  0))
    assert(clusterMap( 6) === List( 6,  5,  0))
    assert(clusterMap( 7) === List( 7,  5,  0))
    assert(clusterMap( 8) === List( 8,  5,  0))
    assert(clusterMap( 9) === List( 9,  5,  0))
    assert(clusterMap(10) === List(10, 10,  0))
    assert(clusterMap(11) === List(11, 10,  0))
    assert(clusterMap(12) === List(12, 10,  0))
    assert(clusterMap(13) === List(13, 10,  0))
    assert(clusterMap(14) === List(14, 10,  0))
    assert(clusterMap(15) === List(15, 15,  0))
    assert(clusterMap(16) === List(16, 15,  0))
    assert(clusterMap(17) === List(17, 15,  0))
    assert(clusterMap(18) === List(18, 15,  0))
    assert(clusterMap(19) === List(19, 15,  0))
    assert(clusterMap(20) === List(20, 20,  0))
    assert(clusterMap(21) === List(21, 20,  0))
    assert(clusterMap(22) === List(22, 20,  0))
    assert(clusterMap(23) === List(23, 20,  0))
    assert(clusterMap(24) === List(24, 20,  0))
    assert(clusterMap(25) === List(25, 25, 25))
    assert(clusterMap(26) === List(26, 26, 25))
    assert(clusterMap(27) === List(27, 27, 25))
    assert(clusterMap(28) === List(28, 28, 25))
    assert(clusterMap(29) === List(29, 29, 25))
    assert(clusterMap(30) === List(30, 30, 30))
    assert(clusterMap(31) === List(31, 31, 30))
    assert(clusterMap(32) === List(32, 32, 30))
    assert(clusterMap(33) === List(33, 33, 30))
    assert(clusterMap(34) === List(34, 34, 30))
    assert(clusterMap(35) === List(35, 35, 35))
    assert(clusterMap(36) === List(36, 36, 35))
    assert(clusterMap(37) === List(37, 37, 35))
    assert(clusterMap(38) === List(38, 38, 35))
    assert(clusterMap(39) === List(39, 39, 35))
    assert(clusterMap(40) === List(40, 40, 40))
    assert(clusterMap(41) === List(41, 41, 40))
    assert(clusterMap(42) === List(42, 42, 40))
    assert(clusterMap(43) === List(43, 43, 40))
    assert(clusterMap(44) === List(44, 44, 40))
  }
}
