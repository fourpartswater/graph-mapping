//scalastyle:off
/**
  * Code is an adaptation of https://github.com/usc-cloud/hadoop-louvain-community with the original done by
  * Copyright 2013 University of California, licensed under the Apache License, version 2.0,
  * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0
  *
  * There are some minor fixes, which I have attempted to resubmit back to the baseline version.
  */
package software.uncharted.graphing.clustering.usc.reference



import scala.collection.mutable.Buffer
import scala.reflect.ClassTag



class Vector[T: ClassTag] (defaultValue: T, initialCapacity: Int = 0) {
  private val buffer = Buffer[T]()
  for (i <- 0 until initialCapacity) buffer += defaultValue

  def asSeq: Seq[T] = buffer

  def update(index: Int, t: T) {
    while (index >= buffer.size) buffer += defaultValue
    buffer(index) = t
  }

  def clear = buffer.clear

  def size = buffer.size

  def toArray = buffer.toArray
}
//scalastyle:on
