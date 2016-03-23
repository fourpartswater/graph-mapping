package software.uncharted.graphing.utilities

import java.text.ParseException

import org.scalatest.FunSuite

class StringParserTestSuite extends FunSuite {
  test("Integer parsing") {
    val parser = new StringParser("1 -1 12345 6789 3000000000")
    assert(1 === parser.nextInt())
    assert(-1 === parser.nextInt())
    assert(12345 === parser.nextInt())
    assert(6789 === parser.nextInt())

    intercept[NumberFormatException] {
      parser.nextInt()
    }
  }

  test("Long parsing") {
    val parser = new StringParser("1 -1 12345 6789 3000000000 -3000000000")
    assert(1L === parser.nextLong())
    assert(-1L === parser.nextLong())
    assert(12345L === parser.nextLong())
    assert(6789L === parser.nextLong())
    assert(3000000000L === parser.nextLong())
    assert(-3000000000L === parser.nextLong())
  }

  test("Float parsing") {
    assert(12.0f === new StringParser("12.0   abc").nextFloat())
    assert(12.0f === new StringParser("12").nextFloat())
    assert(12.0f === new StringParser("12.").nextFloat())
    assert(12.0f === new StringParser("12.0").nextFloat())
    assert(12.0f === new StringParser("12.0E0").nextFloat())
    assert(12.0f === new StringParser("12.0e-0").nextFloat())

    val parser = new StringParser("12 12. 12.1 12.1E1 12.1e-1 -12 -12. -12.1 -12.1e1 -12.1E-1 ")
    assert(12.0f   === parser.nextFloat())
    assert(12.0f   === parser.nextFloat())
    assert(12.1f   === parser.nextFloat())
    assert(121.0f  === parser.nextFloat())
    assert(1.21f   === parser.nextFloat())
    assert(-12.0f  === parser.nextFloat())
    assert(-12.0f  === parser.nextFloat())
    assert(-12.1f  === parser.nextFloat())
    assert(-121.0f === parser.nextFloat())
    assert(-1.21f  === parser.nextFloat())
  }

  test("Double parsing") {
    assert(12.0 === new StringParser("12.0   abc").nextDouble())
    assert(12.0 === new StringParser("12").nextDouble())
    assert(12.0 === new StringParser("12.").nextDouble())
    assert(12.0 === new StringParser("12.0").nextDouble())
    assert(12.0 === new StringParser("12.0E0").nextDouble())
    assert(12.0 === new StringParser("12.0e-0").nextDouble())

    val parser = new StringParser("12 12. 12.1 12.1E1 12.1e-1 -12 -12. -12.1 -12.1e1 -12.1E-1 ")
    assert(12.0   === parser.nextDouble())
    assert(12.0   === parser.nextDouble())
    assert(12.1   === parser.nextDouble())
    assert(121.0  === parser.nextDouble())
    assert(1.21   === parser.nextDouble())
    assert(-12.0  === parser.nextDouble())
    assert(-12.0  === parser.nextDouble())
    assert(-12.1  === parser.nextDouble())
    assert(-121.0 === parser.nextDouble())
    assert(-1.21  === parser.nextDouble())
  }

  test("Quoted substring parsing") {
    assert("abc" === new StringParser(""""abc"""").nextString())
    assert("abc" === new StringParser("""   "abc"""").nextString())
    assert("abc" === new StringParser("""   "abc"   """).nextString())
    assert(""" abc " \ def """ === new StringParser("""   " abc \" \\ def "   """).nextString())

    assert(""""foo"""" === new StringParser(""""\"foo\""""").nextString())
  }

  test("Test constant token skipping") {
    val parser = new StringParser("""abc123 def 456ghi"jkl"""")
    parser.eat("abc")
    assert(123 === parser.nextInt())
    parser.eat("def")
    assert(456L === parser.nextLong())
    parser.eat("ghi")
    assert("jkl" === parser.nextString())

    intercept[ParseException] {
      new StringParser("   abc").eat("abc", false)
    }
  }

  test("Test sequence parsing") {
    val parser = new StringParser("""(1, 2, 3, 4, 3, 2, 1) ["abc":"def":"ghi"] 1:2:3 1 2 3 4 5 """)
    assert(List(1, 2, 3, 4, 3, 2, 1) === parser.nextSeq(() => parser.nextInt(), Some("("), Some(","), Some(")")).toList)
    assert(List("abc", "def", "ghi") === parser.nextSeq(() => parser.nextString(), Some("["), Some(":"), Some("]")).toList)
    assert(List(1, 2, 3) === parser.nextSeq(() => parser.nextInt(), None, Some(":"), None).toList)
    assert(List(1, 2, 3, 4, 5) == parser.nextSeq(() => parser.nextInt(), None, None, None))

    val p2 = new StringParser("""true false false true""")
    assert(List(true, false, false, true) === p2.nextSeq(() => p2.nextBoolean(), None, None, None))
  }
}
