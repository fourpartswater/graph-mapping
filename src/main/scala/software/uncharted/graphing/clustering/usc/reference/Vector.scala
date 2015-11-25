package software.uncharted.graphing.clustering.usc.reference



import scala.collection.mutable.Buffer



/**
 * Created by nkronenfeld on 11/24/2015.
 */
class Vector[T >: Null] (initialCapacity: Int = 0) {
  private val buffer = Buffer[T]()
  for (i <- 0 until initialCapacity) buffer += null

  def asSeq: Seq[T] = buffer

  def update(index: Int, t: T) {
    while (index >= buffer.size) buffer += null
    buffer(index) = t
  }

  def clear = buffer.clear

  def size = buffer.size
}
