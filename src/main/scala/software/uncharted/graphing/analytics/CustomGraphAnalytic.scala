/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.analytics



import java.io.{File, FileNotFoundException}

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import software.uncharted.salt.core.analytic.Aggregator



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
  * @tparam T The intermediate type of data being aggregated during cluster aggregation
  */
trait CustomGraphAnalytic[T] extends Serializable {
  /**
    * The name of the analytic.  This will be used as the key under which the value is listed in the final tiled data.
    */
  val name: String

  /**
    * Get the name, but altered so it can be used as the name of a column in a SQL table
    */
  def getColumnName = name.replace(' ', '_')

  /** The input column for the raw cluster aggregator */
  val column: Int
  /**
    * An aggregator that takes the values of the above column, converts it into something meaningful, and aggregates
    * the values by cluster.
    */
  val aggregator: Aggregator[String, T, String]

  /**
    * Take two processed, aggregated values, and determine the minimum value of the pair.
    */
  def min (left: String, right: String): String

  /**
    * Take two processed, aggregated values, and determine the maximum value of the pair.
    */
  def max (left: String, right: String): String

  /**
    * Initialize a new instance of the aggregator using configuration parameters.
    * @param configs Configuration to use for initialization
    * @return Configured instance
    */
  def initialize(configs: Config): CustomGraphAnalytic[T]
}
object CustomGraphAnalytic {
  /**
    * Given a class name, construct an instance of the Custom Graph Analytic that class name describes, using a
    * no-argument constructor.
    *
    * @param className The class name of the custom graph analytic to construct
    * @return An instance of the named analytic.
    */
  def apply (className: String, configName: String): CustomGraphAnalytic[_] = {
    val rawClass = this.getClass.getClassLoader.loadClass(className)
    if (!classOf[CustomGraphAnalytic[_]].isAssignableFrom(rawClass))
      throw new IllegalArgumentException(s"$className is not a CustomGraphAnalytic")
    val analyticClass = rawClass.asInstanceOf[Class[CustomGraphAnalytic[_]]]
    var instance = analyticClass.getConstructor().newInstance()

    val file = new File(configName)
    if (file.exists()) {
      val configs = ConfigFactory.parseReader(scala.io.Source.fromFile(file).bufferedReader()).resolve()
      instance = instance.initialize(configs)
    } else if (!configName.isEmpty()) {
      throw new FileNotFoundException("Configuration file not found: " + configName)
    }

    instance
  }

  /**
    * Given a list of custom graph analytics, determine all columns needed by any of them
    */
  def determineColumns (analytics: Seq[CustomGraphAnalytic[_]]): Seq[Int] = {
    analytics.map(_.column).distinct.sorted
  }

  /**
    * Figure out which columns, of the list of columns needed by all custom analytics, a given analytic needs
    */
  def savedAnalyticColumn (allNeeded: Seq[Int], analytic: CustomGraphAnalytic[_]): Int = {
    allNeeded.indexOf(analytic.column)
  }
}
