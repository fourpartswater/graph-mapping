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
package software.uncharted.graphing.clustering.modifiedreference



import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.FunSuite



class PerformanceTestSuite extends FunSuite {
  test("Array creation vs. range.toArray") {
    val I = 10
    val N = 10000000

    var arrayCreation = 0L
    var toArray = 0L
    for (i <- 0 until I) {
      val startTime1 = System.nanoTime()
      val a = new Array[Int](N)
      for (j <- 0 until N) a(j) = j
      val endTime1 = System.nanoTime()
      val b = (0 until N).toArray
      val endTime2 = System.nanoTime()
      arrayCreation = arrayCreation + (endTime1 - startTime1)
      toArray = toArray + (endTime2 - endTime1)
    }
    println("Average time to create a %d-sized array by array creation: %.3fms".format(N, arrayCreation/1000000.0))
    println("Average time to create a %d-sized array by using range.toArray: %.3fms".format(N, toArray/1000000.0))
  }

  test("formatted vs. concattenated vs. simplified printing") {
    val N = 1000000
    val out = new PrintStream(new ByteArrayOutputStream())
    var formatted = 0L
    var concatennated = 0L
    var simplified = 0L
    for (i <- 0 until N) {
      val t0 = System.nanoTime()
      out.println("foo\t%d\t%d\t%d\t%f\t%s".format(0, 1, 2, 3.0, "four"))
      val t1 = System.nanoTime()
      out.println("foo\t"+0+"\t"+1+"\t"+2+"\t"+3.0+"\t"+"four")
      val t2 = System.nanoTime()
      out.print("foo\t")
      out.print(0)
      out.print("\t")
      out.print(1)
      out.print("\t")
      out.print(2)
      out.print("\t")
      out.print(3.0)
      out.print("\t")
      out.println("four")
      val t3 = System.nanoTime()
      formatted = formatted + (t1 - t0)
      concatennated = concatennated + (t2 - t1)
      simplified = simplified + (t3 - t2)
    }
    println("Total time to print %d strings using format: %.3fms".format(N, formatted/1000000.0))
    println("Total time to print %d strings using concattenation: %.3fms".format(N, concatennated/1000000.0))
    println("Total time to print %d strings using simple prints: %.3fms".format(N, simplified/1000000.0))
  }
}
