package software.uncharted.graphing.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
  * A group of operations to aid in debugging errors in pipelines.
  */
class DebugGraphOperations {
  def countRDDRowsOp[T] (message: String = "Number of rows: ")(data: RDD[T]): RDD[T] = {
    println(message+data.count)
    data
  }

  def countDFRowsOp (message: String = "Number of rows: ")(data: DataFrame): DataFrame = {
    println(message + data.count)
    data
  }

  def nRDDLinesOp[T] (rows: Int, listMessage: String = "First %d rows:", rowMessage: String = "Row %d: ")(data: RDD[T]): RDD[T] = {
    println(listMessage.format(rows))
    data.take(rows).zipWithIndex.foreach{case (text, row) =>
      println(rowMessage.format(row) + text)
    }
    data
  }

  def nDFLinesOp (rows: Int, listMessage: String = "First %d rows:", rowMessage: String = "Row %d: ")(data: DataFrame): DataFrame = {
    println(listMessage.format(rows))
    data.take(rows).zipWithIndex.foreach{case (text, row) =>
      println(rowMessage.format(row) + text)
    }
    data
  }
}
