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



import java.io.PrintStream

import scala.collection.mutable.{Buffer => MutableBuffer, Map => MutableMap}



object SimpleProfiling {
  private val root = new SimpleProfiler("")

  def splitKey (key: String): Array[String] = key.split("\\.")
  def register (key: String): Unit = {
    try {
      root.register(splitKey(key): _*)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(e.getMessage.replace("<>", key))
    }
  }

  def finish (key: String): Unit = {
    try {
      root.finish(splitKey(key): _*)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(e.getMessage.replace("<>", key))
    }
  }

  def report (to: PrintStream): Unit = {
    root.report(to, "")
  }
}

class SimpleProfiler (key: String) {
  private var stats = MutableMap[String, Long]()
  private var registrations = MutableMap[String, Long]()
  private var subProfilers = MutableMap[String, SimpleProfiler]()
  private var order = MutableBuffer[String]()

  def register (key: String*): Unit = {
    if (0 == key.length) throw new IllegalArgumentException("Attempt to register null key <>")
    else {
      if (order.find(_ == key(0)).isEmpty) order += key(0)
      if (1 == key.length) {
        if (registrations.contains(key(0))) throw new IllegalArgumentException("Attempt to double-register key <>")
        else registrations(key(0)) = System.nanoTime()
      } else {
        subProfilers.getOrElseUpdate(key(0), new SimpleProfiler(key(0))).register(key.drop(1): _*)
      }
    }
  }

  def finish (key: String*): Unit = {
    val endTime = System.nanoTime()

    if (0 == key.length) throw new IllegalArgumentException("Attempt to finish null key")
    else if (1 == key.length) {
      registrations.remove(key(0)) match {
        case Some(startTime) =>
          stats(key(0)) = stats.getOrElse(key(0), 0L) + (endTime - startTime)
        case None =>
          throw new IllegalArgumentException("Attempt to finish unregistered key <>")
      }
    } else {
      subProfilers.getOrElseUpdate(key(0), new SimpleProfiler(key(0))).finish(key.drop(1):_*)
    }
  }

  def report (to: PrintStream, prefix: String): Unit = {
    order.foreach{key =>
      stats.get(key) match {
        case Some(value) =>
          to.println("%s%s: %.3fms".format(prefix, key, (value/1000000.0)))
        case None =>
          to.println("%s%s:".format(prefix, key))
      }
      subProfilers.get(key).foreach(sub => sub.report(to, prefix+(" "*key.length)+"."))
    }
  }
}
