//scalastyle:off
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
