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



object JSONParserUtils {
  def getBoolean(json: Map[String, Any], key: String): Option[Boolean] =
    json.get(key).map(_ match {
      case b: Boolean => b
    })

  def getInt(json: Map[String, Any], key: String): Option[Int] =
    json.get(key).map(_ match {
      case b: BigInt => b.toInt
      case i: Int => i
      case l: Long => l.toInt
      case f: Float => f.toInt
      case d: Double => d.toInt
    })

  def getLong(json: Map[String, Any], key: String): Option[Long] =
    json.get(key).map(_ match {
      case b: BigInt => b.toLong
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toLong
      case d: Double => d.toLong
    })

  def getFloat(json: Map[String, Any], key: String): Option[Float] =
    json.get(key).map(_ match {
      case i: Int => i.toFloat
      case l: Long => l.toFloat
      case f: Float => f.toFloat
      case d: Double => d.toFloat
    })

  def toDouble (a: Any): Double =
    a match {
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
    }

  def getDouble(json: Map[String, Any], key: String): Option[Double] =
    json.get(key).map(toDouble)

  def getString(json: Map[String, Any], key: String): Option[String] =
    json.get(key).map(_ match {
      case s: String => s
    })

  def getSeq[T](json: Map[String, Any], key: String, extractor: Any => T): Option[Seq[T]] =
    json.get(key).map(_ match {
      case a: Array[Any] => a.map(contents => extractor(contents)).toSeq
      case l: List[Any] => l.map(contents => extractor(contents))
    })
}
