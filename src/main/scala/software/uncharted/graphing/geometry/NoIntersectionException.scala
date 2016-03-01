package software.uncharted.graphing.geometry

/**
  * Created by nkronenfeld on 2016-02-29.
  */
class NoIntersectionException (message: String, cause: Throwable) extends Exception(message, cause) {
  def this() = this(null, null)

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(null, cause)
}
