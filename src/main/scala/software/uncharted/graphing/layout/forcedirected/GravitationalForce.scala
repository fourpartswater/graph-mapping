package software.uncharted.graphing.layout.forcedirected

class GravitationalForce (gravity: Double, center: V2, k_inv: Double) extends Force {
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2]): Unit = {
    for (n <- nodes.indices) {
      val delta = center - nodes(n).geometry.position
      val distance = delta.length - nodes(n).geometry.radius
      if (distance > 0) {
        displacements(n) = displacements(n) + delta * (distance * k_inv * gravity)
      }
    }
  }
}
