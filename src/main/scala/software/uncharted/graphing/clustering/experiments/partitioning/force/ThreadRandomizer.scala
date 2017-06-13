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
