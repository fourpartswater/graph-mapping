package software.uncharted.graphing.export

import org.scalatest.FunSuite

class ClusteredNodeTestSuite extends FunSuite {
  test("Clustered Node Id") {
    val node = new ClusteredNode("id", "coordx", "coordy", "radius", "parentid", "parentx", "parenty", "parentr", "numnodes", "degree", 10, "hierarchy", Array())
    assert(node.levelId() === "id_c_9")

    val leaf = new ClusteredNode("id", "coordx", "coordy", "radius", "parentid", "parentx", "parenty", "parentr", "numnodes", "degree", 0, "hierarchy", Array())
    assert(leaf.levelId() === "id"
  }
  test("Clustered Node Hierarchy") {
    val node = new ClusteredNode("id", "coordx", "coordy", "radius", "parentid", "parentx", "parenty", "parentr", "numnodes", "degree", 10, "hierarchy", Array())
    assert(node.inclusiveHierarchy() === "hierarchy|id_c_9")

    val top = new ClusteredNode("id", "coordx", "coordy", "radius", "parentid", "parentx", "parenty", "parentr", "numnodes", "degree", 12, "", Array())
    assert(top.inclusiveHierarchy() === "id_c_11")
  }
}
