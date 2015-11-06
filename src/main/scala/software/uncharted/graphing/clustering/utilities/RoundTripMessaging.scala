/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.clustering.utilities



import org.apache.spark.graphx._

import scala.reflect.ClassTag


/**
 * A class to send a round-trip message from every node to some subset of its neighbors, and to receive back a response
 *
 * Created by nkronenfeld on 11/3/2015.
 *
 * @tparam N The node type of the graph in which to send the message
 * @tparam E The edge type of the graph in which to send the message
 * @tparam M The type of the message to send
 * @tparam R The type of the response to return
 */
trait RoundTripMessaging[N, E, M, R] {
}

/**
 * An object that can send round-trip messages between nodes, in parallel
 */
object RoundTripMessaging {
  /**
   * Give every node a chance to send a message to one other node (that should be its neighbor), and to receive a
   * message back.
   *
   * The process to do this is:
   *    For each node:
   *      calculate and record a destination and a message
   *    send messages
   *      note that they are sent, so unsent messages (i.e., messages not to a neighbor) can be noted as errors
   *      Note: How to prevent double-sends with multiple edges between the nodes?
   *    aggregate messages
   *      Predetermined number of messages to go through
   *    process messages
   *    send answers
   *      aggregation should be irrelevant: since each node only sent one message, it should receive at most one answer
   *    Process answers
   *      recorded where message was sent, so it can tell the "no answer" case
   *      Also recorded if sent, so "no receiver" error can be noted.
   *
   * @param graph
   * @param scorer A function that, given a source node, a destination node, and the edge linking them, determines
   *               the importance of sending a message from the source to the destination
   * @param messager A function that, given a source node, a destination node, and the edge linking them, produces
   *                 a message to send from the source to the destination
   * @tparam N The node type of the graph in which to send the message
   * @tparam E The edge type of the graph in which to send the message
   * @tparam M The type of the message to send
   * @tparam R The type of the response to return
   */
  def bestNeighborMessage[N: ClassTag, E, M: ClassTag, R: ClassTag] (graph: Graph[N, E],
                                                                     scorer: (N, E, N) => Double,
                                                                     messager: (N, E, N) => M,
                                                                     responder: ((VertexId, (N, (Double, VertexId, Option[M]))), (Double, VertexId, Option[M])) => R) =
  {
    val calcNeighbor = new BestNeighborDetermination[N, E, M](scorer, messager)
    val sendMessage = new MessageSender[N, E, M, R](responder)

    // Calculate the destination and message for each node
    val graphWithMessages = calcNeighbor(graph)
    val graphWithResponses = sendMessage(graphWithMessages)

    // Send messages to neighbors
  }
}


/**
 * Determine the best neighbor to which to send a message for each vertex.  The work here, determining which neighbor
 * is best, can be done in two ways:
 *
 * mergeNeighborInfo can pass on only the better of its two inputs, and  determineBestNeighbor can simply extracts the
 * single vertex ID from the info it is given.
 *
 * Alternatively, mergeNeighborInfo can combine the multiple inputs into one collected input (in which case T had better
 * be a collection type), and determineBestNeighbor can take the collected information and compare with more complete
 * information.
 *
 * The former is better, if it is possible, as it involves less object creation (of collection objects)
 *
 * @param scorer A function that, given a source node, a destination node, and the edge linking them, determines
 *               the importance of sending a message from the source to the destination
 * @param messager A function that, given a source node, a destination node, and the edge linking them, produces
 *                 a message to send from the source to the destination
 * @tparam N (Node) The type of node in the graph
 * @tparam E (Edge) The type of edge in the graph
 * @tparam M (Message) The type of the message to send from one node to another
 */
class BestNeighborDetermination[N: ClassTag, E, M: ClassTag] (scorer: (N, E, N) => Double, messager: (N, E, N) => M)
  extends NeighborInfoVertexTransformation[
    N,
    E,
    (Double, VertexId, Option[M]),
    (N, (Double, VertexId, Option[M]))
  ]
{
  override val dct = implicitly[ClassTag[Data]]
  override val oct = implicitly[ClassTag[Output]]

  override def getEdgeInfo(context: EdgeContext[N, E, Data]): Unit = {
    def processNeighbor: ((N, E, VertexId, N) => (Double, VertexId, Option[M])) =
      (source, edge, destinationId, destination) => {
        (
          scorer(source, edge, destination),
          destinationId,
          Some(messager(source, edge, destination))
        )
      }

    context.sendToSrc(processNeighbor(context.srcAttr, context.attr, context.dstId, context.dstAttr))
    context.sendToDst(processNeighbor(context.dstAttr, context.attr, context.srcId, context.srcAttr))
  }

  override def mergeEdgeInfo(a: Data, b: Data): Data = if (a._1 > b._1) a else b

  override def mergeVertexInfo(vid: VertexId,
                               vertexData: N,
                               edgeDataOption: Option[(Double, VertexId, Option[M])]):
  (N, (Double, VertexId, Option[M])) = {
    (vertexData, edgeDataOption.getOrElse((0.0, -1L, None)))
  }
}

class MessageSender[N: ClassTag, E, M: ClassTag, R: ClassTag] (processMessage: ((VertexId, (N, (Double, VertexId, Option[M]))), (Double, VertexId, Option[M])) => R)
  extends NeighborInfoVertexTransformation[
    (N, (Double, VertexId, Option[M])),
    E,
    (Double, VertexId, Option[M]),
    (N, (Double, VertexId, Option[M]), Option[R])
  ]
{
  override val dct = implicitly[ClassTag[Data]]
  override val oct = implicitly[ClassTag[Output]]
  override val fields = TripletFields.Src
  type Message = (Double, VertexId, Option[M])
  type Node = (N, Message)
  type Response = (N, Message, Option[R])

  override def getEdgeInfo(context: EdgeContext[Node, E, Message]): Unit = {
    val (data, message) = context.srcAttr
    val (targetId, weight, payload) = message
    if (targetId == context.dstId) {
      // Sub in the source ID for the destination Id
      context.sendToDst((weight, context.srcId, payload))
    }
  }

  // Only send the most important message
  override def mergeEdgeInfo(a: Message, b: Message): Message = if (a._1 > b._1) a else b

  override def mergeVertexInfo(vid: VertexId, vertexData: Node, edgeDataOption: Option[Message]): Response = {
    val response = edgeDataOption.map{edgeData =>
      processMessage((vid, vertexData), edgeData)
    }
    (vertexData._1, vertexData._2, response)
  }
}

