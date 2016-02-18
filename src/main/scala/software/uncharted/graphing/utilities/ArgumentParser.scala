package software.uncharted.graphing.utilities



import scala.collection.mutable.{Map => MutableMap}



class ArgumentParser (args: Array[String]) {
  private val usage = MutableMap[String, (String, Option[_])]()
  def getStringOption (key: String, description: String, defaultValue: Option[String]): Option[String] = {
    usage(key) = (description, defaultValue)
    val result = args.sliding(2).find(p => p(0) == "-"+key).map(_(1))
    if (result.isEmpty) defaultValue else result
  }
  def getStrings (key: String, description: String): Seq[String] = {
    usage(key) = (description, None)
    args.sliding(2).filter(p => p(0) == "-"+key).map(_(1)).toSeq
  }
  def getString (key: String, description: String, defaultValue: String) =
    getStringOption(key, description, Some(defaultValue)).get

  def getIntOption(key: String, description: String, defaultValue: Option[Int]) =
    getStringOption(key, description, defaultValue.map(_.toString)).map(_.toInt)
  def getInt (key: String, description: String, defaultValue: Int) =
    getIntOption(key, description, Some(defaultValue)).get

  def getIntSeq (key: String, description: String, defaultValue: Option[Seq[Int]]): Seq[Int] =
    getStringOption(key, description, defaultValue.map(_.mkString(","))).get.split(",").map(_.toInt).toSeq
}
