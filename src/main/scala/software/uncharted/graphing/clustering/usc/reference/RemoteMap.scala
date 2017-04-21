//scalastyle:off
/**
  * Code is an adaptation of https://github.com/usc-cloud/hadoop-louvain-community with the original done by
  * Copyright 2013 University of California, licensed under the Apache License, version 2.0,
  * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0
  *
  * There are some minor fixes, which I have attempted to resubmit back to the baseline version.
  */
package software.uncharted.graphing.clustering.usc.reference

case class RemoteMap (var source: Int, var sink: Int, var sinkPart: Int)
//scalastyle:on
