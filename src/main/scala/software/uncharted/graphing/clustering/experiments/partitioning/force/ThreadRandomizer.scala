package software.uncharted.graphing.clustering.experiments.partitioning.force

import scala.util.Random

/**
  * Created by nkronenfeld on 2016-01-16.
  */
object ThreadRandomizer {
  private val threadLocalRandom = new ThreadLocal[Random]() {
    override protected def initialValue():Random = {
      new Random
    }
  }
  def get: Random = threadLocalRandom.get
}
