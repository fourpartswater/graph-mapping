/**
 * This code is copied and translated from https://sites.google.com/site/findcommunities
 *
 * This means it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
 * we can't distribute it without permission - though as a translation, with some optimization for readability in
 * scala, it may be a gray area.
 */
package software.uncharted.graphing.clustering.reference



import java.util.Date
import scala.collection.mutable.{Buffer, Map => MutableMap}
import scala.util.Random



/**
 *
 * @param g The graph to cluster
 * @param nb_pass number of pass for one level computation
 *                if -1, compute as many pass as needed to increas modularity
 * @param min_modularity a new pass is computed if the last one has generated an increase greater than min_modularity
 *                       if 0, even a minor increase is enough to go for one more pass
 */
class Community (g: Graph, nb_pass: Int, min_modularity: Double) {
  val size = g.nb_nodes
  val n2c = (0 until size).toArray
  val tot = n2c.map(n => g.weighted_degree(n))
  val in = n2c.map(n => g.nb_selfloops(n))
  val neigh_weight = n2c.map(n => -1.0)
  val neigh_pos = n2c.map(n => 0)
  var neigh_last: Int = 0

  def remove(node: Int, comm: Int, dnodecomm: Double): Unit = {
    tot(comm) = tot(comm) - g.weighted_degree(node)
    in(comm) = in(comm) - (2 * dnodecomm + g.nb_selfloops(node))
    n2c(node) = -1
  }

  def insert(node: Int, comm: Int, dnodecomm: Double): Unit = {
    tot(comm) = tot(comm) + g.weighted_degree(node)
    in(comm) = in(comm) + (2 * dnodecomm + g.nb_selfloops(node))
    n2c(node) = comm
  }

  def modularity_gain(node: Int, comm: Int, dnodecomm: Double, w_degree: Double): Double = {
    val totc = tot(comm)
    val degc = w_degree
    val m2 = g.total_weight
    val dnc = dnodecomm
    (dnc - totc * degc / m2)
  }

  def modularity: Double = {
    var q = 0.0
    val m2 = g.total_weight

    for (i <- 0 until size) {
      if (tot(i) > 0) {
        val tm = tot(i) / m2
        q += in(i) / m2 - tm * tm
      }
    }

    q
  }

  def neigh_comm(node: Int): Unit = {
    for (i <- 0 until neigh_last) {
      neigh_weight(neigh_pos(i)) = -1
    }
    val degree = g.nb_neighbors(node)
    neigh_pos(0) = n2c(node)
    neigh_weight(neigh_pos(0)) = 0
    neigh_last = 1
    val neighbors = g.neighbors(node)
    for (i <- 0 until degree) {
      val (neighbor, neighbor_weight) = neighbors.next
      val neighbor_community = n2c(neighbor)
      if (neighbor != node) {
        if (neigh_weight(neighbor_community) == -1.0) {
          neigh_weight(neighbor_community) = 0.0
          neigh_pos(neigh_last) = neighbor_community
          neigh_last += 1
        }
        neigh_weight(neighbor_community) += neighbor_weight
      }
    }
  }

  def one_level: Boolean = {
    var improvement = false
    var nb_moves: Int = 0
    var nb_pass_done: Int = 0
    var new_mod = modularity
    var cur_mod = new_mod

    // Randomize the order of vertex inspection
    val random_order = (0 until size).toArray
    val randomizer = new Random()
    for (i <- 0 until (size - 1)) {
      val rand_pos = randomizer.nextInt(size - i) + i
      val tmp = random_order(i)
      random_order(i) = random_order(rand_pos)
      random_order(rand_pos) = tmp
    }

    // repeat while
    //   there is an improvement of modularity
    //   or there is an improvement of modularity greater than a given epsilon
    //   or a predefined number of pass have been done
    do {
      cur_mod = new_mod
      nb_moves = 0
      nb_pass_done += 1

      // for each node: remove the node from its community and insert it in the best community
      for (node_tmp <- 0 until size) {
        val node = random_order(node_tmp)
        val node_comm = n2c(node)
        val w_degree = g.weighted_degree(node)

        // computation of all neighboring communities of current node
        neigh_comm(node)
        // remove node from its current community
        remove(node, node_comm, neigh_weight(node_comm))

        // compute the nearest community for node
        // default choice for future insertion is the former community
        var best_comm = node_comm
        var best_nblinks = 0.0
        var best_increase = 0.0
        for (i <- 0 until neigh_last) {
          val increase = modularity_gain(node, neigh_pos(i), neigh_weight(neigh_pos(i)), w_degree)
          if (increase > best_increase) {
            best_comm = neigh_pos(i)
            best_nblinks = neigh_weight(neigh_pos(i))
            best_increase = increase
          }
        }

        // insert node in the nearest community
        insert(node, best_comm, best_nblinks)

        if (best_comm != node_comm)
          nb_moves += 1
      }

      var total_tot = 0.0
      var total_in = 0.0
      for (i <- 0 until tot.size) {
        total_tot += tot(i)
        total_in += in(i)
      }

      new_mod = modularity
      if (nb_moves > 0)
        improvement = true;

    } while (nb_moves > 0 && new_mod - cur_mod > min_modularity)

    improvement
  }

  private def getRenumbering: (Array[Int], Int) = {
    val renumber = (0 until size).map(n => -1).toArray
    for (node <- 0 until size)
      renumber(n2c(node)) += 1

    var last = 0
    for (i <- 0 until size) {
      if (renumber(i) != -1) {
        renumber(i) = last
        last += 1
      }
    }

    (renumber, last)
  }

  def display_partition: Unit = {
    val (renumber, last) = getRenumbering
    for (i <- 0 until size) {
      println(i + " " + renumber(n2c(i)))
    }
  }

  def partition2graph_binary: Graph = {
    val (renumber, last) = getRenumbering

    // Compute communities
    val comm_nodes = Buffer[Buffer[Int]]()
    for (node <- 0 until size) {
      val n = renumber(n2c(node))
      while (comm_nodes.size < n+1) comm_nodes += Buffer[Int]()
      comm_nodes(n) += node
    }

    // Compute weighted graph
    val nb_nodes = comm_nodes.size
    val degrees = (0 until nb_nodes).map(n => 0).toArray
    val links = Buffer[Int]()
    val weights = Buffer[Double]()

    val comm_deg = comm_nodes.size
    for (comm <- 0 until comm_deg) {
      val m = MutableMap[Int, Double]()

      val comm_size = comm_nodes(comm).size
      for (node <- 0 until comm_size) {
        val neighbors = g.neighbors(comm_nodes(comm)(node))
        val deg = g.nb_neighbors(comm_nodes(comm)(node))
        for (i <- 0 until deg) {
          val (neigh, neigh_weight) = neighbors.next
          val neigh_comm = renumber(n2c(neigh))
          m(neigh_comm) = m.get(neigh_comm).getOrElse(0.0) + neigh_weight
        }
      }
      degrees(comm) = if (0 == comm) m.size else degrees(comm-1) + m.size
      m.map{case (neighbor, weight) =>
          links += neighbor
          weights += weight
      }
    }

    new Graph(degrees.toSeq, links.toSeq, Some(weights.toSeq))
  }
}



class CommunityHarness {
  def run (graph: Graph,
           passes: Int = -1,
           precision: Double = 0.000001,
           display_level: Int = -2,
           verbose: Boolean = false) = {
    val time_begin = System.currentTimeMillis()
    if (verbose) {
      println("Start time: "+new Date(time_begin))
      println("Input:")
      graph.display
    }

    var g = graph
    var c = new Community(g, passes, precision)
    var improvement = true
    var mod = c.modularity
    var new_mod = mod
    var level = 0

    do {
      improvement = c.one_level
      new_mod = c.modularity
      level += 1
      if (level == display_level)
        g.display
      if (display_level == -1)
        c.display_partition

      g = c.partition2graph_binary
      c = new Community(g, -1, precision)

      if (verbose) {
        println("  modularity increased from " + mod + " to " + new_mod)
        g.display
      }
      mod = new_mod
      if (verbose)
        println("  end computation")
    } while (improvement)

    val time_end = System.currentTimeMillis()
    if (verbose) {
      println("End time: "+new Date(time_end))
      println("Total duration: "+((time_end - time_begin)/1000.0)+" seconds")
    }
    println("Final modularity: "+new_mod)
  }
}