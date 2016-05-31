/**
  * This code is copied and translated from https://sites.google.com/site/findcommunities
  *
  * This means it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
  * we can't distribute it without permission - though as a translation, with some optimization for readability in
  * scala, it may be a gray area.
  */
package software.uncharted.graphing.clustering.unithread.reference



class Convert {

}


object Convert {
  var infile: Option[String] = None
  var outfile: Option[String] = None
  var outfile_w: Option[String] = None
  var weighted = false
  var do_renumber = false

  def usage (prog_name: String, more: String): Unit = {
    println(more)
    println("usage: " + prog_name + " -i input_file -o outfile [-r] [-w outfile_weight]")
    println
    println("read the graph and convert it to binary format.")
    println("-r\tnodes are renumbered from 0 to nb_nodes-1 (the order is kept).")
    println("-w filename\tread the graph as a weighted one and writes the weights in a separate file.")
    println("-h\tshow this usage message.")

    System.exit(0)
  }

  def parse_args (args: Array[String]): Unit = {
    var i = 0;
    while (i < args.size) {
      if (args(i).startsWith("-")) {
        args(i).substring(1).toLowerCase match {
          case "i" => {
            i = i + 1
            infile = Some(args(i))
          }
          case "o" => {
            i = i + 1
            outfile = Some(args(i))
          }
          case "w" => {
            weighted = true
            i = i + 1
            outfile_w = Some(args(i))
          }
          case "r" => do_renumber = true
          case _ => usage("convert", "Unknown option: "+args(i))
        }
      }
      i = i + 1
    }
  }

  def main (args: Array[String]): Unit = {
    parse_args(args)
    var g = GraphEdges(infile.get, weighted)
    if (do_renumber)
      g = g.renumber(weighted)
    g.display_binary(outfile.get, outfile_w, weighted)
  }
}
