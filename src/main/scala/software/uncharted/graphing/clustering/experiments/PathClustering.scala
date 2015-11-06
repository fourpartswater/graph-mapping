package software.uncharted.graphing.clustering.experiments

import org.apache.spark.graphx._
import software.uncharted.graphing.clustering.utilities.NeighborInfoVertexTransformation

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.ClassTag


/**
 * Created by nkronenfeld on 10/28/2015.
 */
class PathClustering {
  def checkClusters[VD, ED] (graph: Graph[VD, ED]): Graph[(VD, VertexId), ED] = {
    val withSelfWeights = graph.mapVertices{case (vid, data) => (data, MutableMap(vid -> 1))}
    val ct = new ConnectivityTransferer[VD, ED]
    val iter1 = ct.transformVertexInfo(withSelfWeights)
    val iter2 = ct.transformVertexInfo(iter1)
    val iter3 = ct.transformVertexInfo(iter2)

    val cc = new ConnectivityChooser[VD, ED]
    cc.transformVertexInfo(iter3)
  }

}

class ConnectivityChooser[VD, ED]
  extends NeighborInfoVertexTransformation[
    (VD, MutableMap[VertexId, Int]),
    ED,
    (Int, Set[VertexId]),
    (VD, VertexId)] {
  val dct = implicitly[ClassTag[Data]]
  val oct = implicitly[ClassTag[Output]]

  def getEdgeInfo (context: EdgeContext[(VD, MutableMap[VertexId, Int]), ED, Data]): Unit = {
    def numOverSize (n: Int, src: (VD, MutableMap[VertexId, Int])): Int = {
      val cutoff = n * n
      src._2.map(_._2).filter(n =>
        n >= cutoff
      ).size
    }

    // Take our mutable map, and find the top frequency
    def sendTopMsgs (src: (VD, MutableMap[VertexId, Int]),
                     dst: VertexId,
                     send: Data => Unit): Unit = {
      val srcMax = src._2.map(_._2).fold(0)(_ max _)
      val topSizeCandidate = math.sqrt(srcMax).floor.toInt
      val topSize = (topSizeCandidate to 2 by -1).filter { n =>
        numOverSize(n, src) >= n
      }.headOption
      topSize.foreach { n: Int =>
        val topCandidates = src._2.filter(_._2 >= n * n).map(_._1).toSet
        if (topCandidates.contains(dst)) send((n, topCandidates))
      }
    }

    sendTopMsgs(context.srcAttr, context.dstId, context.sendToDst)
    sendTopMsgs(context.dstAttr, context.srcId, context.sendToSrc)
  }

  def mergeEdgeInfo (a: Data, b: Data): Data =
    (a._1 min b._1, a._2 intersect b._2)

  def mergeVertexInfo (vid: VertexId,
                        inputVertexInfo: (VD, MutableMap[VertexId, Int]),
                        edgeInfoOption: Option[Data]): (VD, VertexId) = {
    edgeInfoOption.map{case (n, set) =>
      if (set.size >= n) {
        (inputVertexInfo._1, set.toSeq.sorted.last)
      } else {
        (inputVertexInfo._1, vid)
      }
    }.getOrElse((inputVertexInfo._1, vid))
  }
}
class ConnectivityTransferer[VD, ED]
  extends NeighborInfoVertexTransformation [
    (VD, MutableMap[VertexId, Int]),
    ED,
    MutableMap[VertexId, Int],
    (VD, MutableMap[VertexId, Int])] {
  val dct = implicitly[ClassTag[Data]]
  val oct = implicitly[ClassTag[Output]]

  def getEdgeInfo (context: EdgeContext[(VD, MutableMap[VertexId, Int]), ED, Data]): Unit = {
    context.sendToDst(context.srcAttr._2)
    context.sendToSrc(context.dstAttr._2)
  }

  def mergeEdgeInfo (a: Data, b: Data): Data = {
    val result = MutableMap[VertexId, Int]()
    a.foreach{case (key, value) => result(key) = value}
    b.foreach{case (key, value) => result(key) = result.get(key).getOrElse(0) + value}

    result
  }

  def mergeVertexInfo (vid: VertexId,
                       inputVertexInfo: (VD, MutableMap[VertexId, Int]),
                       edgeInfoOption: Option[Data]): (VD, MutableMap[VertexId, Int]) = {
    (inputVertexInfo._1,
      edgeInfoOption.map(edgeInfo =>
        // merge info from other nodes into the info for this node
        mergeEdgeInfo(edgeInfo, inputVertexInfo._2)
      ).getOrElse(inputVertexInfo._2)
    )
  }
}
