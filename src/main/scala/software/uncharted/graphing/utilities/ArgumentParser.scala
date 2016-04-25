package software.uncharted.graphing.utilities


import scala.collection.mutable.{Map => MutableMap}


class ArgumentParser(args: Array[String]) {
  private val argumentDescriptions = MutableMap[String, (String, Option[_])]()

  def getStringOption(key: String, description: String, defaultValue: Option[String]): Option[String] = {
    argumentDescriptions(key) = (description, defaultValue)
    val result = args.sliding(2).find(p => p(0) == "-" + key).map(_ (1))
    if (result.isEmpty) defaultValue else result
  }

  def getStrings(key: String, description: String): Seq[String] = {
    argumentDescriptions(key) = (description, None)
    args.sliding(2).filter(p => p(0) == "-" + key).map(_ (1)).toSeq
  }

  def getString(key: String, description: String, defaultValue: String) =
    getStringOption(key, description, Some(defaultValue)).get

  def getDoubleOption(key: String, description: String, defaultValue: Option[Double]) =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toDouble)

  def getDouble(key: String, description: String, defaultValue: Double) =
    getDoubleOption(key, description, Some(defaultValue)).get

  def getIntOption(key: String, description: String, defaultValue: Option[Int]) =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toInt)

  def getInt(key: String, description: String, defaultValue: Int) =
    getIntOption(key, description, Some(defaultValue)).get

  def getIntSeq(key: String, description: String, defaultValue: Option[Seq[Int]]): Seq[Int] =
    getStringOption(key, description, defaultValue.map(_.mkString(","))).get.split(",").map(_.toInt).toSeq

  def getBooleanOption(key: String, description: String, defaultValue: Option[Boolean]) =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toBoolean)

  def getBoolean(key: String, description: String, defaultValue: Boolean) =
    getBooleanOption(key, description, Some(defaultValue)).get

  def usage: Unit = {
    argumentDescriptions.keySet.toList.sorted.foreach{key =>
      val (description, defaultValue) = argumentDescriptions(key)
      println(s"${key}\t${defaultValue}\t${description}")
    }
  }
}
