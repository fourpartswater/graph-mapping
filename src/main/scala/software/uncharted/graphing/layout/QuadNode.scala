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
  * A Quad Node Object used for QuadTree Decomposition
  *
  * Adapted from the 'FFDLayouter' code in ApertureJS
  *
  */
class QuadNode(box: (Double, Double, Double, Double)) {

  private var _boundingBox: (Double, Double, Double, Double) = box	// bounding box (x,y of lower left corner, width, height)
  private var _NW: QuadNode = _
  private var _NE: QuadNode = _
  private var _SW: QuadNode = _
  private var _SE: QuadNode = _

  private var _nChildren: Int = 0
  private var _centerOfMass: (Double, Double) = _
  private var _size: Double = _

  private var _data: QuadNodeData = null


  def getBounds: (Double, Double, Double, Double) = {
    _boundingBox
  }

  def getNW: QuadNode = {
    _NW
  }

  def getNE: QuadNode = {
    _NE
  }

  def getSW: QuadNode = {
    _SW
  }

  def getSE: QuadNode = {
    _SE
  }

  def getData: QuadNodeData = {
    _data
  }

  def getNumChildren: Int = {
    _nChildren
  }

  def getCenterOfMass: (Double, Double) = {
    _centerOfMass
  }

  def getSize: Double = {
    _size
  }

  def setNW(nw: QuadNode) {
    _NW = nw
  }

  def setNE(ne: QuadNode) {
    _NE = ne
  }

  def setSW(sw: QuadNode) {
    _SW = sw
  }

  def setSE(se: QuadNode) {
    _SE = se
  }

  def setData(data: QuadNodeData) = {
    _data = data
  }

  def setNumChildren(n: Int) = {
    _nChildren = n
  }

  def setCenterOfMass(centre: (Double, Double)) = {
    _centerOfMass = centre
  }

  def setSize(size: Double) = {
    _size = size
  }

  def incrementChildren() {
    _nChildren += 1
  }

  // Tests if a given point is inside the bounding rectangle of this quadnode
  def isPointInBounds(x: Double, y: Double): Boolean = {
    x >= _boundingBox._1 && x <= _boundingBox._1+_boundingBox._3 &&
      y >= _boundingBox._2 && y <= _boundingBox._2+_boundingBox._4
  }

}


/**
  * Used to contain the data being stored in this quadtree node.
  *
  * Adapted from the 'FFDLayouter' code in ApertureJS
  *
  */
class QuadNodeData(x: Double, y: Double, id: Long, size: Double) {

  private var _x: Double = x
  private var _y: Double = y
  private var _id: Long = id
  private var _size: Double = size

  def getX: Double = {
    _x
  }

  def getY: Double = {
    _y
  }

  def getSize: Double = {
    _size
  }

  def getId: Long = {
    _id
  }
}
