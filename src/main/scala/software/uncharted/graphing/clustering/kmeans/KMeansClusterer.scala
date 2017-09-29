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



import scala.collection.mutable.{Map => MutableMap}
import scala.util.Random
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}



/**
  * A component for clustering datasets using K-Means clustering.
  */
object KMeansClusterer {
  import software.uncharted.graphing.clustering.utilities.VectorUtilities._

  /* returns vector size, min, and max */
  private def vectorStats (vectors: Seq[Vector]): (Int, Vector, Vector) = {
    assert(vectors.length > 0, "Attempt to perform K-Means clustering on empty input")
    val sizes = vectors.map(_.size)
    assert(sizes.min == sizes.max, "K-Means must be called with a constant number of features for each data point")

    (sizes.min, vectors.reduce(min), vectors.reduce(max))
  }

  /* Round a number to the nearest 1/nth */
  private def round (nth: Int) = {
    (d: Double) => (d * nth).round.toDouble / nth
  }

  private def newRandomVectorFromLargestGroup (r: Random, closestCentroids: Seq[Int], vectors: Seq[Vector]) = {
    val groupCount = MutableMap[Int, Int]()
    closestCentroids.foreach(n => groupCount(n) = groupCount.getOrElse(n, 0) + 1)
    val (lgIndex, lgSize) = groupCount.maxBy(_._2)
    val lg = (vectors zip closestCentroids).filter(_._2 == lgIndex).map(_._1)

    () => {
      val largest = lgIndex
      val next = lg(r.nextInt(lg.length))
      next
    }
  }


  /**
    * A single-threaded, in-line implementation of K-means clustering.
    *
    * Group a set of vectors into a set of K clusters, where K &isin; possibleKs
    *
    * Algorithm based on that found in
    * From http://stanford.edu/~cpiech/cs221/handouts/kmeans.html
    *
    * @param possibleKs The possible values K may take
    * @param featureExtractorFcn A function to retrieve the feature vector from a given record
    * @param attemptsPerK The number of clustering attempts to make at each value of K.  More attempts gives a
    *                     greater probability of achieving a good clustering.
    * @param maxIterations The maximum number of iterations to use before quiting
    * @param seed A random seed
    * @param data The input data
    * @tparam T The type of the input records
    * @return The original records, each augmented with the cluster into which they belong, and their distance from
    *         the center of the cluster.
    */
  def clusterSetLocally[T] (possibleKs: Seq[Int],
                            featureExtractorFcn: T => Vector,
                            attemptsPerK: Int = 10,
                            maxIterations: Int = 1000,
                            seed: Long = System.nanoTime())
                           (data: Seq[T]): Seq[(T, Int, Double)] = {
    val r = new Random(seed)
    val unscaledVectorData = data.map(d => (d, featureExtractorFcn(d)))
    val (numFeatures, minV, maxV) = vectorStats(unscaledVectorData.map(_._2))
    val scaledVectors = unscaledVectorData.map { case (d, v) => fromRange(minV, maxV)(v) }

    val results = for (k <- possibleKs) yield {
      (1 to attemptsPerK).map { n =>
        // Initialize centroids randomly
        var centroids = new Array[Vector](k)
        var closestCentroids: Seq[Int] = Seq()
        val newRandomVector = () => randomVector(r)(minV, maxV)
        for (i <- 0 until k) {
          centroids(i) = newRandomVector()
        }

        // Initialize bookkeeping
        var iterations = 0
        var oldCentroids: Array[Vector] = null

        while (!shouldStop(oldCentroids, centroids, iterations, maxIterations)) {
          // Save old centroids for convergence test, bookkeeping
          oldCentroids = centroids
          iterations = iterations + 1

          // Assign centroids to each data point
          closestCentroids = getClosestCentroids(scaledVectors, centroids)

          // Determine new centroids based on data
          centroids = getCentroids(scaledVectors, closestCentroids, k,
            newRandomVectorFromLargestGroup(r, closestCentroids, scaledVectors), r)
        }

        KResult(k, centroids, closestCentroids, calculateDk(scaledVectors, closestCentroids, centroids))
      }.minBy(_.Sk) // Pick the best attempt at this value of K
    }

    val k = bestKByRatio(results.map(r => (r.k, r.Sk)), () => numFeatures)
    val bestResults = results.find(_.k == k).get

    data.zipWithIndex.map { case (d, i) =>
      val cluster = bestResults.clusters(i)
      val distance = math.sqrt(Vectors.sqdist(featureExtractorFcn(d), toRange(minV, maxV)(bestResults.centroids(cluster))))
      (d, cluster, distance)
    }
  }

  /**
    * A single-threaded, in-line, hierarchical implementation of K-means clustering.
    *
    * Divide a set of vectors into K clusters, where K &isin; possibleKs.  Divide each cluster into K subclusters
    * similarly, until every remaining undivided set is smaller than max(possibleKs).
    *
    * Each resultant cluster will be labelled with the ID of the node most central to that cluster.
    *
    * The heirarchy will be filled out so that leaf nodes are at the same depth.
    *
    * @param possibleKs The possible values K may take
    * @param featureExtractorFcn A function to retrieve the feature vector from a given record
    * @param attemptsPerK The number of clustering attempts to make at each value of K.  More attempts gives a
    *                     greater probability of achieving a good clustering.
    * @param maxIterations The maximum number of iterations to use before quiting
    * @param seed A random seed
    * @param data The input data
    * @tparam T The type of the input records
    * @return The original records, each augmented with the clusters to which they belong, at each level of clustering
    */
  def heirarchicalClusterSetLocally[T] (possibleKs: Seq[Int],
                                        featureExtractorFcn: T => Vector,
                                        idExtractorFcn: T => Int,
                                        attemptsPerK: Int = 10,
                                        maxIterations: Int = 1000,
                                        seed: Long = System.nanoTime())
                                       (data: Seq[T]): Seq[(T, Seq[Int])] = {
    val maxSize = possibleKs.max
    var avgSize = data.length.toDouble

    // Add a root cluster
    val firstLevelResult = clusterSetLocally(possibleKs, featureExtractorFcn, attemptsPerK, maxIterations, seed)(data)
    if (1 == firstLevelResult.map(_._2).toSet.size) {
      // Couldn't find a sub-cluster - it's all one.
      val centralId = idExtractorFcn(firstLevelResult.minBy(_._3)._1)
      data.map(t => (t, Seq(idExtractorFcn(t), centralId)))
    } else {
      firstLevelResult
        .groupBy(_._2)
        .toSeq
        .flatMap { case (cluster, clusterData) =>
          // Find the datum closest to the cluster centroid
          val centralId = idExtractorFcn(clusterData.minBy(_._3)._1)


          if (clusterData.length > maxSize) {
            heirarchicalClusterSetLocally[(T, Int, Double)](
              possibleKs,
              d => featureExtractorFcn(d._1),
              d => idExtractorFcn(d._1),
              attemptsPerK,
              maxIterations,
              seed
            )(clusterData)
              .map { case ((datum, _, _), subCluster) =>
                (datum, subCluster :+ centralId)
              }
          } else {
            clusterData.map { case (datum, _, _) =>
              (datum, Seq(idExtractorFcn(datum), centralId))
            }
          }
        }
    }
  }

  /*
   * Helper function for clusterSetLocally - decide if the current itteration is good enough
   */
  private def shouldStop (oldCentroids: Array[Vector], newCentroids: Array[Vector],
                          iterations: Int, maxIterations: Int): Boolean = {
    // We ignore the use of null (a scalastyle issue) because it's a safety check only - we never
    // assign null, so this should have no far-reaching implications.
    if (iterations > maxIterations) {
      true
    } else if (null == oldCentroids || null == newCentroids) { // scalastyle:ignore
      false
    } else {
      (oldCentroids zip newCentroids)
        .map { case (oc, nc) => oc.equals(nc) }
        .reduceOption(_ && _)
        .getOrElse(false)
    }
  }

  /*
   * Helper function for clusterSetLocally - determine the nearest centroid for each vector in the input set
   */
  private def getClosestCentroids[T] (data: Seq[Vector], centroids: Array[Vector]): Seq[Int] = {
    val indexedCentroids = centroids.zipWithIndex
    data.map { d =>
      indexedCentroids.minBy(c => Vectors.sqdist(c._1, d))._2
    }
  }

  /*
   * Helper function for clusterSetLocally - Determine the centroids of a set of clusters
   */
  private def getCentroids[T] (data: Seq[Vector],
                               currentClustering: Seq[Int],
                               k: Int,
                               newRandomVector: () => Vector,
                               r: Random): Array[Vector] = {
    val results = new Array[(Int, Option[Vector])](k)
    for (i <- 0 until k) {
      results(i) = (0, Option.empty[Vector])
    }

    for (i <- data.indices) {
      val v = data(i)
      val cluster = currentClustering(i)
      val (currentN, currentSum) = results(cluster)
      results(cluster) = (
        currentN + 1,
        Some(currentSum.map(cs => add(cs, v)).getOrElse(v))
        )
    }

    val scaledResults = results.map {
      _ match {
        case (0, _) =>
          ("random", newRandomVector())
        case (n, Some(sum)) =>
          ("scaled", scale(sum, 1.0 / n))
        case (n, None) =>
          ("error", newRandomVector())
      }
    }

    scaledResults.map(_._2)
  }

  /*
   * Helper function for clusterSetLocally - determine how good a match for the dataset a set of centroids is
   */
  private def calculateDk (data: Seq[Vector], closestCentroids: Seq[Int], centroids: Array[Vector]): Double = {
    var D = 0.0
    var clusterSizes = new Array[Int](centroids.length)
    for (i <- closestCentroids) {
      clusterSizes(i) = clusterSizes(i) + 1
    }
    for (i <- data.indices) {
      val v = data(i)
      val cluster = closestCentroids(i)
      val centroid = centroids(cluster)
      D = D + 2 * clusterSizes(cluster) * Vectors.sqdist(v, centroid)
    }
    D
  }


  /**
    * Clusters the dataset into one of several possible numbers of clusters, picking the best number
    * for the dataset, using the Spark MLLib K-Means clustering
    *
    * @param possibleKs The possible numbers of clusters into which to cluster the dataset
    * @param featureColumn The column in which to find the vector on which clustering for each point of data
    * @param resultColumn The column into which to put the cluster for each point of data
    * @param distanceColumn The column into which to put the distance of each datum to the center of its cluster; None
    *                       not to record this information
    * @param seed The random seed to be used for clustering
    * @param data The dataset
    * @tparam T The dataset type
    * @return A similar dataset, augmented with a column that contains a prefered cluster ID for each point
    */
  def clusterSetDistributed[T](possibleKs: Seq[Int],
                               featureColumn: String = "features",
                               resultColumn: String = "prediction",
                               distanceColumn: Option[String] = None,
                               seed: Long = System.nanoTime())
                              (data: Dataset[T]): DataFrame = {
    assert(possibleKs.length > 0)

    val kmeans = new KMeans()
    kmeans.setSeed(seed)
    kmeans.setFeaturesCol(featureColumn)
    kmeans.setPredictionCol(resultColumn)

    val models = possibleKs.map { k =>
      kmeans.setK(k)
      val model = kmeans.fit(data)
      (k, model, model.computeCost(data))
    }

    val results = if (1 == models.length) {
      models
    } else {
      val validModels = models.filter { case (k, model, cost) =>
        // Eliminate exact fits - they are obviously overfit
        cost > 0
      }
      if (0 == validModels.length) {
        Seq(models.minBy(_._1))
      } else {
        validModels.sortBy(_._1)
      }
    }

    val numFeatures = () => data.select(featureColumn).rdd.map(_.getAs[Vector](0).size).max
    val model = bestModel(results, numFeatures)
    val withCluster = model.transform(data)
    distanceColumn.map(dc => addDistance(resultColumn, featureColumn, dc)(withCluster, model))
      .getOrElse(withCluster)
  }

  /**
    * A distributed, hierarchical implementation of K-means clustering.
    *
    * Divide a set of vectors into K clusters, where K &isin; possibleKs.  Divide each cluster into K subclusters
    * similarly, until every remaining undivided set is smaller than max(possibleKs).
    *
    * Each resultant cluster will be labelled with the ID of the node most central to that cluster.
    *
    * The heirarchy will be filled out so that leaf nodes are at the same depth.
    *
    * The top level us clustered using distributed Spark K-Means clustering.  Each subsequent cluster is then
    * broken down locally.  The implication is this will break down if initial clusters are too big to fit
    * locally on a single machine - we should continue to work using distributed clustering until the clusters are
    * small enough to fit on a single machine.  This is, however, problematic, because there is no heirarchical
    * k-means clustering in MLLib, so we have to run on each sub-cluster sequentially, rather than in parallel.
    *
    * @param possibleKs The possible numbers of clusters into which to cluster the dataset
    * @param idColumn The column in which to find the ID of each datum (so that this ID can be propagated up through
    *                 the heirarchy, to indicate the most central node of each cluster).
    * @param featureColumn The column in which to find the vector on which clustering for each point of data
    * @param resultColumn The column into which to put the cluster for each point of data
    * @param attemptsPerK The number of clustering attempts to make at each value of K when breaking up upper-level
    *                     clusters.  More attempts gives a greater probability of achieving a good clustering.
    * @param maxIterations The maximum number of iterations to use before quiting (when breaking up upper-level
    *                      clusters)
    * @param seed A random seed
    * @param data The dataset
    * @tparam T The dataset type
    * @return A similar dataset, augmented with a column that contains a prefered cluster ID for each point
    */
  def heirarchicalClusterSetDistributed[T] (possibleKs: Seq[Int],
                                            idColumn: String = "id",
                                            featureColumn: String = "features",
                                            resultColumn: String = "prediction",
                                            attemptsPerK: Int = 10,
                                            maxIterations: Int = 1000,
                                            seed: Long = System.nanoTime())
                                           (data: Dataset[T]): DataFrame = {
    import data.sparkSession.implicits._

    val topLevelResultColumn = resultColumn + "_tmp"
    val distanceColumn = resultColumn + "_dist"

    // Figure out our top level clusters
    val topLevelData = clusterSetDistributed(possibleKs, featureColumn, topLevelResultColumn, Some(distanceColumn), seed = seed)(data)

    // Determine lower-level clusters in parallel
    val minK = possibleKs.min
    val maxK = possibleKs.max
    val ic = topLevelData.schema.fieldIndex(idColumn)
    val rc = topLevelData.schema.fieldIndex(topLevelResultColumn)
    val vc = topLevelData.schema.fieldIndex(featureColumn)
    val dc = topLevelData.schema.fieldIndex(distanceColumn)
    val fullData = topLevelData.rdd
      .groupBy(_.getInt(rc))
      .flatMap { case (cluster, rows) =>
        val data = rows.map(r => (r, r.getInt(ic), r.getDouble(dc))).toSeq
        // Get the ID of the datum closest to the centroid
        val centralId = data.minBy(_._3)._2

        if (data.length <= maxK) {
          data.map { case (row, topCluster, id) =>
            val id = row.getInt(ic)
            Row.fromSeq(row.toSeq :+ Array(id, centralId))
          }
        } else {
          heirarchicalClusterSetLocally[(Row, Int, Double)](
            possibleKs,
            r => r._1.getAs[Vector](vc),
            r => r._2,
            attemptsPerK,
            maxIterations,
            seed
          )(data)
            .map { case ((row, _, _), subCluster) =>
              val cluster = (subCluster :+ centralId).toArray
              Row((row.toSeq :+ cluster):_*)
            }
        }
      }

    // Pad out depth so that the tree is the same depth everywhere
    val maxDepth = fullData.map(row => row.getAs[Array[Int]](row.length - 1).length).max
    val paddedData = fullData.map { row =>
      val clusters = row.getAs[Array[Int]](row.length - 1)
      if (clusters.length == maxDepth) {
        row
      } else {
        val paddedCluster = Array[Int]().padTo(maxDepth - clusters.length, clusters.head) ++ clusters
        Row((row.toSeq.dropRight(1) :+ paddedCluster):_*)
      }
    }
    val augmentedSchema = topLevelData.schema.add(resultColumn, ArrayType(IntegerType))
    data.sparkSession.createDataFrame(paddedData, augmentedSchema).drop(topLevelResultColumn, distanceColumn)
  }

  // Add a distance column to a dataframe
  private def addDistance (clusterColumn: String, featureColumn: String, distanceColumn: String)
                          (data: DataFrame, model: KMeansModel): DataFrame = {
    val centers = model.clusterCenters
    val getDistance = functions.udf((cluster: Int, features: Vector) => {
      math.sqrt(Vectors.sqdist(features, centers(cluster)))
    })
    data.withColumn(distanceColumn, getDistance(data(clusterColumn), data(featureColumn)))
  }

  // Pick the best model
  private def bestModel (from: Seq[(Int, KMeansModel, Double)], numFeatures: () => Int): KMeansModel = {
    val bestK = bestKByRatio(from.map(info => (info._1, info._3)), numFeatures)
    from.find(_._1 == bestK).get._2
  }

  // Function to find the best result when there is no clear elbow.
  private def bestOfEquals (from: Seq[(Int, Double)]) = {
    // Take the first of all results that are within an order of magnitude of the smallest result
    val minResult = from.map(_._2).min
    from.filter(_._2 < minResult * 10)(0)
  }

  // Pick the model with the best K from a sequence of fitted models
  // Input is Seq[(K, model, cost)]
  private[kmeans] def bestKByRatio (kInfos: Seq[(Int, Double)], numFeaturesFcn: () => Int): Int = {
    // Get the best K
    if (kInfos.length < 4) {
      // Not enough points to have a clear elbow
      bestOfEquals(kInfos)._1
    } else {
      // Find the elbow
      val pairs = kInfos.filter(_._2 != 0.0).sliding(2).map(ab => (ab(0), ab(1))).toList
      val ratios = pairs.zipWithIndex.map { case ((a, b), i) =>
        (i, a._2 / b._2)
      }
      val mean = ratios.map(_._2).sum / ratios.length
      val stddev = math.sqrt(ratios.map { case (i, v) => (mean - v) * (mean - v) }.sum / ratios.length)
      if (ratios.length == 0) {
        // all the same - just pick the smallest
        kInfos.map(_._1).min
      } else {
        val best = ratios.maxBy(_._2)._1
        if (ratios(best)._2 > mean + stddev) {
          pairs(best)._2._1
        } else {
          // No clear elbow - just pick the best one
          bestOfEquals(kInfos.filter(_._2 != 0.0))._1
        }
      }
    }
  }

  // Selection of K according to:
  //   https://datasciencelab.wordpress.com/2013/12/27/finding-the-k-in-k-means-clustering/
  //   http://www.ee.columbia.edu/~dpwe/papers/PhamDN05-kmeans.pdf
  //
  // Inputs assumed sorted in increasing K order
  // My own calculation of a best K (bestKByRatio) by elbow detection seems to work better
  // than this, so this is not currently used.
  private[kmeans] def bestKByGapStatistic (kInfos: Seq[(Int, Double)], numFeaturesFcn: () => Int): Int = {
    val numFeatures = numFeaturesFcn()
    val maxK = kInfos.map(_._1).max

    val alpha = new Array[Double](maxK)
    alpha(0) = 0.0
    alpha(1) = 1.0 - 0.75 / numFeatures
    for (i <- 2 until maxK) {
      alpha(i) = alpha(i - 1) + (1.0 - alpha(i - 1)) / 6.0
    }

    val fK = new Array[Double](kInfos.length)
    for (i <- kInfos.indices) {
      if (0 == i) {
        fK(i) = 1
      } else {
        val (k, sk) = kInfos(i)
        val (k_, sk_) = kInfos(i - 1)
        val fullAlpha = (k_ until k).map(kk => alpha(kk - 1)).product
        fK(i) = (sk / sk_) / fullAlpha
      }
    }

    kInfos(fK.zipWithIndex.minBy(_._1)._2)._1
  }
}

private[kmeans] case class KResult (k: Int, centroids: Array[Vector], clusters: Seq[Int], Sk: Double)
