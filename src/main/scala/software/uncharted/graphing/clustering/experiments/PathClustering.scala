package software.uncharted.graphing.clustering.experiments

import org.apache.spark.graphx._
import software.uncharted.graphing.clustering.utilities.VertexCalculation

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.ClassTag


/**
 * Created by nkronenfeld on 10/28/2015.
 */
class PathClustering {
  def checkClusters[VD, ED] (graph: Graph[VD, ED]): Graph[(VD, VertexId), ED] = {
    val withSelfWeights = graph.mapVertices{case (vid, data) => (data, MutableMap(vid -> 1))}
//    val ct = new ConnectivityTransferer[VD, ED]
//    val iter1 = ct.transformVertexInfo(withSelfWeights)
//    val iter2 = ct.transformVertexInfo(iter1)
//    val iter3 = ct.transformVertexInfo(iter2)
    val iter1 = ConnectivityTransferer(withSelfWeights)
    val iter2 = ConnectivityTransferer(iter1)
    val iter3 = ConnectivityTransferer(iter2)

    ConnectivityChooser(iter3)
  }
}

object ConnectivityChooser extends VertexCalculation[MutableMap[VertexId, Int]] {
  override type Data = (Int, Set[VertexId])
  val dct = implicitly[ClassTag[Data]]
  override val defaultData: Data = (0, Set[VertexId]())

  override def getEdgeInfo(context: EdgeContext[MutableMap[VertexId, PartitionID], Double, Data]): Option[(Option[Data], Option[Data])] = {
    def numOverSize (n: Int, src: MutableMap[VertexId, Int]): Int = {
      val cutoff = n * n
      src.map(_._2).filter(n =>
        n >= cutoff
      ).size
    }

    // Take our mutable map, and find the top frequency
    def sendTopMsgs (src: MutableMap[VertexId, Int],
                     dst: VertexId): Option[Data]= {
      val srcMax = src.map(_._2).fold(0)(_ max _)
      val topSizeCandidate = math.sqrt(srcMax).floor.toInt
      val topSize = (topSizeCandidate to 2 by -1).filter { n =>
        numOverSize(n, src) >= n
      }.headOption

      topSize.flatMap { n: Int =>
        val topCandidates = src.filter(_._2 >= n * n).map(_._1).toSet
        if (topCandidates.contains(dst)) Some((n, topCandidates))
        else None
      }
    }

    Some(
      sendTopMsgs(context.dstAttr, context.srcId),
      sendTopMsgs(context.srcAttr, context.dstId)
    )
  }

  /** mergeMsg function for aggregateMessages */
  override def mergeEdgeInfo (a: Data, b: Data): Data =
    (a._1 min b._1, a._2 intersect b._2)

  private def mergeVertexInfo[VD] (vid: VertexId,
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


  def apply[VD, ED] (graph: Graph[(VD, MutableMap[VertexId, Int]), ED]): Graph[(VD, VertexId), ED] = {
    type VDI = (VD, MutableMap[VertexId, Int])
    type VDO = (VD, VertexId)
    val extractFcn: (VertexId, VDI) => MutableMap[VertexId, Int] = (id, vertexData) => vertexData._2
    val mergeFcn: (VertexId, VDI, Option[Data]) => VDO = (id, vertexData, edgeData) => mergeVertexInfo[VD](id, vertexData, edgeData)
    calculateModifiedVertexInfo[VDI, VDO, ED](extractFcn, mergeFcn)(graph)
  }
}

object ConnectivityTransferer extends VertexCalculation[MutableMap[VertexId, Int]] {
  override type Data = MutableMap[VertexId, Int]
  val dct = implicitly[ClassTag[Data]]
  override val defaultData: Data = MutableMap[VertexId, Int]()

  override def getEdgeInfo(context: EdgeContext[Data, Double, Data]): Option[(Option[Data], Option[Data])] = {
    Some(Some(context.dstAttr), Some(context.srcAttr))
  }

  override def mergeEdgeInfo (a: Data, b: Data): Data = {
    val result = MutableMap[VertexId, Int]()
    a.foreach{case (key, value) => result(key) = value}
    b.foreach{case (key, value) => result(key) = result.get(key).getOrElse(0) + value}

    result
  }

  private def mergeVertexInfo[VD] (vid: VertexId,
                                   inputVertexInfo: (VD, MutableMap[VertexId, Int]),
                                   edgeInfoOption: Option[Data]): (VD, MutableMap[VertexId, Int]) = {
    (inputVertexInfo._1,
      edgeInfoOption.map(edgeInfo =>
        // merge info from other nodes into the info for this node
        mergeEdgeInfo(edgeInfo, inputVertexInfo._2)
      ).getOrElse(inputVertexInfo._2)
      )
  }

  def apply[VD, ED] (graph: Graph[(VD, Data), ED]): Graph[(VD, Data), ED] = {
    type VDI = (VD, Data)
    type VDO = (VD, Data)
    val extractFcn: (VertexId, VDI) => Data = (id, vertexData) => vertexData._2
    val mergeFcn: (VertexId, VDI, Option[Data]) => VDO = (id, vertexData, edgeData) => mergeVertexInfo[VD](id, vertexData, edgeData)
    calculateModifiedVertexInfo[VDI, VDO, ED](extractFcn, mergeFcn)(graph)
  }
}
