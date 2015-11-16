package software.uncharted.graphing.clustering.usc



import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD

import org.scalatest.FunSuite

import scala.reflect.ClassTag


class SubGraphTestSuite extends FunSuite with SharedSparkContext {
  private def mapToPartitions[K: ClassTag] (data: Map[Int, Iterator[K]], optPartitions: Option[Int]): RDD[K] = {
    val partitions = optPartitions.getOrElse(data.map(_._1).reduce(_ max _))
    sc.parallelize(0 until partitions, partitions).mapPartitionsWithIndex { case (partition, i) =>
      data.get(partition).getOrElse(Iterator[K]())
    }
  }


  test("Siple test of repartitionEqually") {
    val data = mapToPartitions(
      Map(
        1 -> Iterator("a", "b", "c", "d", "e"),
        4 -> Iterator("f", "g"),
        5 -> Iterator("h", "i", "j", "k", "l")
      ),
      Some(8)
    )
    val repartitioned = SubGraph.repartitionEqually(data, 4)
    assert(4 === repartitioned.partitions.size)
    val partitions = repartitioned.mapPartitionsWithIndex{case (p, i) =>
      List((p, i.toList)).iterator
    }.collect.toMap
    assert(List("a", "b", "c") === partitions(0))
    assert(List("d", "e", "f") === partitions(1))
    assert(List("g", "h", "i") === partitions(2))
    assert(List("j", "k", "l") === partitions(3))
    assert(4 === partitions.size)
  }
}
