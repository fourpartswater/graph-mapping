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
package software.uncharted.graphing.layout

/**
  * A simple class to represent a geometric circle, used mostly to describe circular bounds in the layout process
  *
  * @param center The center of the circle
  * @param radius The radius of the circle
  */
case class Circle(center: V2, radius: Double)
