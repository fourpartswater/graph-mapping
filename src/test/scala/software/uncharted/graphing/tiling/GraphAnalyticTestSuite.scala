package software.uncharted.graphing.tiling


import org.scalatest.FunSuite


class GraphAnalyticTestSuite extends FunSuite {
  test("fromStringParser int") {
    val parser = new FromStringParser("0 1 -1 "+Int.MinValue+" "+Int.MaxValue)
    assert(0 === parser.nextInt())
    assert(1 === parser.nextInt())
    assert(-1 === parser.nextInt())
    assert(Int.MinValue === parser.nextInt())
    assert(Int.MaxValue === parser.nextInt())
  }

  test("fromStringParser long") {
    val parser = new FromStringParser("0 1 -1 "+Long.MinValue+" "+Long.MaxValue)
    assert(0 === parser.nextLong())
    assert(1 === parser.nextLong())
    assert(-1 === parser.nextLong())
    assert(Long.MinValue === parser.nextLong())
    assert(Long.MaxValue === parser.nextLong())
  }

  test("fromStringParser double") {
    val parser = new FromStringParser(List(
      0.0, 0.1, 1.0, -0.1, -1.0,
      Double.MinValue, Double.MinPositiveValue, Double.MaxValue
    ).map(_.toString).mkString(" "))

    assert(0.0 === parser.nextDouble())
    assert(0.1 === parser.nextDouble())
    assert(1.0 === parser.nextDouble())
    assert(-0.1 === parser.nextDouble())
    assert(-1.0 === parser.nextDouble())
    assert(Double.MinValue === parser.nextDouble())
    assert(Double.MinPositiveValue === parser.nextDouble())
    assert(Double.MaxValue === parser.nextDouble())

    val parsePP = new FromStringParser("2e2 2.0e2 2E2 2.0E2")
    for (i <- 1 to 4) assert(2.0E2 === parsePP.nextDouble())
    val parsePM = new FromStringParser("2e-2 2.0e-2 2E-2 2.0E-2")
    for (i <- 1 to 4) assert(2.0E-2 === parsePM.nextDouble())
    val parseMP = new FromStringParser("-2e2 -2.0e2 -2E2 -2.0E2")
    for (i <- 1 to 4) assert(-2.0E2 === parseMP.nextDouble())
    val parseMM = new FromStringParser("-2e-2 -2.0e-2 -2E-2 -2.0E-2")
    for (i <- 1 to 4) assert(-2.0E-2 === parseMM.nextDouble())
  }

  test("fromStringParser boolean") {
    val parser = new FromStringParser("true false True False TRUE FALSE")
    assert(true === parser.nextBoolean())
    assert(false === parser.nextBoolean())
    assert(true === parser.nextBoolean())
    assert(false === parser.nextBoolean())
    assert(true === parser.nextBoolean())
    assert(false === parser.nextBoolean())
  }

  test("fromStringParser string") {
    val parser = new FromStringParser(""" "abc" "def\"ghi" "jkl mno\\\" pqr"  """)
    assert("abc" === parser.nextString())
    assert("def\"ghi" === parser.nextString())
    assert("jkl mno\\\" pqr" === parser.nextString())
  }

  test("fromStringParser list") {
    val parser = new FromStringParser(" << 2 3 ||| 4 5 ||| 6 7 >> ")
    assert(List((2, 3L), (4, 5L), (6, 7L)) === parser.nextList[(Int, Long)]("<<", "|||", ">>", p => (p.nextInt(), p.nextLong())).toList)
  }

  test("fromStringParser empty list") {
    val parser = new FromStringParser((" << >> "))
    assert(List[(Int, Long)]() === parser.nextList[(Int, Long)]("<<", "|||", ">>", p => (p.nextInt(), p.nextLong())).toList)
  }

  test("Basic edge construction") {
    val edge = GraphEdgeDestination(3L, (0.75, -0.24), -4L)
    assert(3L === edge.id)
    assert(0.75 === edge.coordinates._1)
    assert(-0.24 === edge.coordinates._2)
    assert(-4L === edge.weight)
  }

  test("edge min") {
    val edge1 = GraphEdgeDestination(1L, (0.5, 0.6), 3L)
    val edge2 = GraphEdgeDestination(2L, (0.4, 0.7), 2L)

    assert(GraphEdgeDestination(1L, (0.4, 0.6), 2L) === (edge1 min edge2))
    assert(GraphEdgeDestination(1L, (0.4, 0.6), 2L) === (edge2 min edge1))
  }

  test("edge max") {
    val edge1 = GraphEdgeDestination(1L, (0.5, 0.6), 3L)
    val edge2 = GraphEdgeDestination(2L, (0.4, 0.7), 2L)

    assert(GraphEdgeDestination(2L, (0.5, 0.7), 3L) === (edge1 max edge2))
    assert(GraphEdgeDestination(2L, (0.5, 0.7), 3L) === (edge2 max edge1))
  }

  test("edge equality") {
    assert(GraphEdgeDestination(1L, (0.5, 0.4), -2L) === GraphEdgeDestination(1L, (0.5, 0.4), -2L))
    assert(GraphEdgeDestination(1L, (0.5, 0.4), -2L) != GraphEdgeDestination(2L, (0.5, 0.4), -2L))
    assert(GraphEdgeDestination(1L, (0.5, 0.4), -2L) != GraphEdgeDestination(1L, (0.51, 0.4), -2L))
    assert(GraphEdgeDestination(1L, (0.5, 0.4), -2L) != GraphEdgeDestination(1L, (0.5, 0.41), -2L))
    assert(GraphEdgeDestination(1L, (0.5, 0.4), -2L) != GraphEdgeDestination(1L, (0.5, 0.4), -1L))
  }

  test ("edge to string") {
    assert("""{"dstID": 1, "dstCoords": [0.5, 0.6], "weight": 4}""" === GraphEdgeDestination(1L, (0.5, 0.6), 4L).toString)
  }

  test("edge from string") {
    assert(GraphEdgeDestination(1L, (0.5, 0.6), 4L) ===
      GraphEdgeDestination.fromString(new FromStringParser("""{"dstID": 1, "dstCoords": [0.5, 0.6], "weight": 4}""")))
  }

  test("edge from string with scientific notation") {
    assert(GraphEdgeDestination(1L, (0.5, 1.23E4), 4L) ===
      GraphEdgeDestination.fromString(new FromStringParser("""{"dstID": 1, "dstCoords": [0.5, 1.23E4], "weight": 4}""")))
  }

  test("edge from string with negatives") {
    assert(GraphEdgeDestination(-1L, (-0.5, -2.6E-45), -4L) ===
      GraphEdgeDestination.fromString(new FromStringParser("""{"dstID": -1, "dstCoords": [-0.5, -2.6E-45], "weight": -4}""")))
  }

  test("edge to/from string round trip") {
    val base = GraphEdgeDestination(-3L, (2354.123, -14.134), 294472L)
    val converted = GraphEdgeDestination.fromString(new FromStringParser(base.toString))
    assert(converted === base)
  }

  test("edge to/from string round trip with maximal/minimal values") {
    val base = GraphEdgeDestination(Long.MinValue, (Double.MinValue, Double.MinPositiveValue), Long.MaxValue)
    val converted = GraphEdgeDestination.fromString(new FromStringParser(base.toString))
    assert(converted === base)
  }

  test("graph community to/from string round trip") {
    val communityIn = GraphCommunity(1, 2L, (3.0, 4.0), 5.0, 6, 7L, "8", true, 10L, (11.0, 12.0), 13.0,
      List(14.0, 15.0, 16.0),
      List(GraphEdgeDestination(17L, (18.0, 19.0), 20L), GraphEdgeDestination(21L, (22.0, 23.0), 24L)),
      List(GraphEdgeDestination(25L, (26.0, 27.0), 28L), GraphEdgeDestination(29L, (30.0, 31.0), 32L))
    )
    val communityOut = GraphCommunity.fromString(new FromStringParser(communityIn.toString))
    assert(communityOut === communityIn)
  }

  test("graph analytic record to/from string round trip") {
    val garIn = GraphAnalyticsRecord(1, List(
      GraphCommunity(2, 2L, (2.0, 2.0), 2.0, 2, 2L, "2", true, 2L, (2.0, 2.0), 2.0,
        List(2.0), List(GraphEdgeDestination(2L, (2.0, 2.0), 2L)), List(GraphEdgeDestination(2L, (2.0, 2.0), 2L))),
      GraphCommunity(3, 3L, (3.0, 3.0), 3.0, 3, 3L, "3", false, 3L, (3.0, 3.0), 3.0,
        List(3.0), List(GraphEdgeDestination(3L, (3.0, 3.0), 3L)), List(GraphEdgeDestination(3L, (3.0, 3.0), 3L))),
      GraphCommunity(4, 4L, (4.0, 4.0), 4.0, 4, 4L, "4", true, 4L, (4.0, 4.0), 4.0,
        List[Double](), List[GraphEdgeDestination](), List[GraphEdgeDestination]())
    ))
    val garOut = GraphAnalyticsRecord.fromString(garIn.toString)
    assert(garOut === garIn)
  }
}
