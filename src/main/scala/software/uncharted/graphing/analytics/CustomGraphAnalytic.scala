package software.uncharted.graphing.analytics



import software.uncharted.salt.core.analytic.Aggregator

import scala.util.parsing.json.JSONObject


/**
  * A custom analytic, that can be run on either nodes or edges.
  *
  * The analytic is calculated on each node or edge of the graph, then aggregated as those nodes and edges are grouped
  * together.
  *
  * Finally, if a node analytic, it is added to the metadata of each node directly; if an edge analytic, it is added to
  * the metadata of the nodes on either end.
  *
  * The intended procedure is as follows:
  * <ol>
  *   <li> Analytics are run on the graph, resulting in columns being added to the node and edge files.  All data
  *        should be written as escaped strings </li>
  *   <li> The convert program passes data through from text to binary format </li>
  *   <li> The cluster program uses the aggregators associated with analytics to consolidate information </li>
  *   <li> The layout program passes data through from cluster files to located cluster files </li>
  *   <li> The metadata tiling pipeline uses the aggregators again to aggregate analytics within a tile, and adds the
  *        aggregated information to the JSON produced for each tile. </li>
  * </ol>
  *
  * The implications of this are:
  * <ul>
  *   <li> Each analytic should be encapsulated in such a way as to allow easy reuse of the same encapsulation between
  *        the various stages of the process.  For the moment, I'm doing this by making each custom analytic a class,
  *        which should have a zero-argument constructor; each stage can then construct it based on class name. </li>
  *   <li> Aggregation is done in two separate places; may need two separate aggregators. </li>
  *   <li> Note type conversions are implicitly contained in the aggregators - note their fixed input and output types.
  *        We will need some simple conversion aggregators to making writing the conversion parts easier. </li>
  * </ul>
  *
  * @tparam CT The type of data being aggregated during cluster aggregation
  * @tparam TT The type of data being aggregated during tile aggregation
  */
trait CustomGraphAnalytic[CT, TT] {
  /**
    * The name of the analytic.  This will be used as the key under which the value is listed in the final tiled data.
    */
  val name: String
  /** The input column for the raw cluster aggregator */
  val column: Int
  /**
    * An aggregator that takes the values of the above column, converts it into something meaningful, and aggregates
    * the values by cluster.
    */
  val clusterAggregator: Aggregator[String, CT, String]
  /**
    * An aggregator that takes values output by the cluster aggregator, aggregates them, and outputs a JSON object that
    * can be added to the metadata tile.
    */
  val tileAggregator: Aggregator[String, TT, JSONObject]
}
object CustomGraphAnalytic {
  /**
    * Given a class name, construct an instance of the Custom Graph Analytic that class name describes, using a
    * no-argument constructor.
    * @param className The class name of the custom graph analytic to construct
    * @return An instance of the named analytic.
    */
  def apply (className: String): CustomGraphAnalytic[_, _] = {
    val rawClass = this.getClass.getClassLoader.loadClass(className)
    if (!classOf[CustomGraphAnalytic[_, _]].isAssignableFrom(rawClass))
      throw new IllegalArgumentException(s"$className is not a CustomGraphAnalytic")
    val analyticClass = rawClass.asInstanceOf[Class[CustomGraphAnalytic[_, _]]]
    analyticClass.getConstructor().newInstance()
  }

  /**
    * Given a list of custom graph analytics, determine all columns needed by any of them
    */
  def determineColumns (analytics: Seq[CustomGraphAnalytic[_, _]]): Seq[Int] = {
    analytics.map(_.column).distinct.sorted
  }

  /**
    * Figure out which columns, of the list of columns needed by all custom analytics, a given analytic needs
    */
  def savedAnalyticColumn (allNeeded: Seq[Int], analytic: CustomGraphAnalytic[_, _]): Int = {
    allNeeded.indexOf(analytic.column)
  }
}