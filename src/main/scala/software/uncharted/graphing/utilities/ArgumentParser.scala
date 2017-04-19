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
package software.uncharted.graphing.utilities



import scala.collection.mutable.{Map => MutableMap}


/**
  * Parse CLI parameters that follow the standard "-key value" format.
  * @param args CLI parameters
  */
class ArgumentParser(args: Array[String]) {
  private val argumentDescriptions = MutableMap[String, (String, Option[_])]()

  /**
    * Get the parameter as a string option, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getStringOption(key: String, description: String, defaultValue: Option[String]): Option[String] = {
    argumentDescriptions(key) = (description, defaultValue)
    val result = args.sliding(2).find(p => p(0) == "-" + key).map(_ (1))
    if (result.isEmpty) defaultValue else result
  }

  /**
    * Get all parameters for the specified key as a sequence of strings. Ex: "-key val1 -key val2"
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @return The sequence of all values for the given parameter.
    */
  def getStrings(key: String, description: String): Seq[String] = {
    argumentDescriptions(key) = (description, None)
    args.sliding(2).filter(p => p(0) == "-" + key).map(_ (1)).toSeq
  }

  /**
    * Get the parameter as a string, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getString(key: String, description: String, defaultValue: String): String =
    getStringOption(key, description, Some(defaultValue)).get

  /**
    * Get the parameter as a double option, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getDoubleOption(key: String, description: String, defaultValue: Option[Double]): Option[Double] =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toDouble)

  /**
    * Get the parameter as a double, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getDouble(key: String, description: String, defaultValue: Double): Double =
    getDoubleOption(key, description, Some(defaultValue)).get

  /**
    * Get the parameter as an integer option, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getIntOption(key: String, description: String, defaultValue: Option[Int]): Option[Int] =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toInt)

  /**
    * Get the parameter as an integer, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getInt(key: String, description: String, defaultValue: Int): Int =
    getIntOption(key, description, Some(defaultValue)).get

  /**
    * Get all parameters for the specified key as a sequence of integers. Ex: "-key val1 -key val2"
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @return The sequence of all values for the given parameter.
    */
  def getIntSeq(key: String, description: String, defaultValue: Option[Seq[Int]]): Seq[Int] =
    getStringOption(key, description, defaultValue.map(_.mkString(","))).get.split(",").map(_.toInt).toSeq

  /**
    * Get the parameter as a boolean option, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getBooleanOption(key: String, description: String, defaultValue: Option[Boolean]): Option[Boolean] =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toBoolean)

  /**
    * Get the parameter as a boolean, defaulting to the supplied value if not found.
    * @param key Name of the parameter
    * @param description Description of the parameter
    * @param defaultValue Default value to use if parameter not found
    * @return The value of the parameter in the CLI parameters, or the default value if not found
    */
  def getBoolean(key: String, description: String, defaultValue: Boolean): Boolean =
    getBooleanOption(key, description, Some(defaultValue)).get

  /**
    * Output the usage message detailing all the parameters.
    */
  def usage: Unit = {
    argumentDescriptions.keySet.toList.sorted.foreach{key =>
      val (description, defaultValue) = argumentDescriptions(key)
      println(s"${key}\t${defaultValue}\t${description}")
    }
  }
}
