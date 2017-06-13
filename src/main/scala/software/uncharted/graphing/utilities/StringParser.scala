/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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



import java.text.ParseException

import scala.collection.mutable.{ListBuffer => MutableBuffer}



/**
  * A class to make parsing strings into objects simple.  Handles tokenization, escaping, and type conversion
  */
//scalastyle:off null multiple.string.literals cyclomatic.complexity
class StringParser (raw: String) {
  val length = raw.length
  var position = 0

  /** Reset to the start of the text, so as to reparse the whole string */
  def reset (): Unit = {
    position = 0
  }

  private def isWhitespace (c: Char): Boolean =
    ' ' == c || '\t' == c || '\n' == c || '\r' == c

  private def eatWhitespace (): Unit = {
    var c = raw.charAt(position)
    while (isWhitespace(c)) {
      position += 1
      c = raw.charAt(position)
    }
  }

  private def advance(): Char = {
    position = position + 1
    if (position < length) { raw.charAt(position) }
    else { 0 }
  }

  private def nextNonDigit(): Char = {
    if (position < length) {
      var c = raw.charAt(position)
      // If past end, c will be 0 (not '0'), which isn't in our bounds, so no separate length check is necessary
      while ('0' <= c && c <= '9') c = advance()
      c
    } else {
      0
    }
  }

  /**
    * Skip past an exact predefined portion of text that should be found at the current position.  An error is thrown
    * if the text is not found.
    *
    * @param text The text to skip
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the text
    */
  def eat (text: String, eatInitialWhitespace: Boolean = true): Unit = {
    if (eatInitialWhitespace) { eatWhitespace() }
    if (raw.startsWith(text, position)) { position += text.length }
    else { throw new ParseException(s"""Didn't find "$text" at position $position""", position) }
  }

  /**
    * Skip past several predefined texts, and any whitespace before and between them.
    * @param texts The texts to skip
    */
  def eat (texts: String*): Unit = {
    texts.foreach(text => eat(text, true))
  }

  /**
    * Like eat, but doesn't advance the position - just tests if the current position starts with the stated string.
    *
    * @param text
    * @param skipInitialWhitespace
    */
  def startsWith (text: String, skipInitialWhitespace: Boolean = true): Boolean = {
    if (position >= length && text != null && text.length > 0) {
      false
    } else {
      var tempPosition = position
      if (skipInitialWhitespace) {
        while (tempPosition < length && isWhitespace(raw.charAt(tempPosition))) tempPosition += 1
      }
      raw.startsWith(text, tempPosition)
    }
  }

  /**
    * Parse and advance past a boolean value that must be found at the current position
    *
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the number
    * @return The next boolean found
    */
  def nextBoolean (eatInitialWhitespace: Boolean = true): Boolean = {
    if (eatInitialWhitespace) { eatWhitespace() }

    def test (offset: Int, lc: Char): Boolean = {
      val c = raw.charAt(position + offset)
      c == lc || c == (lc + ('A' - 'a')).toChar
    }
    if (position <= length - 4 &&
      (test(0, 't') && test(1, 'r') && test(2, 'u') && test(3, 'e'))) {
      position += 4
      true
    } else if (position <= length - 5 &&
      (test(0, 'f') && test(1, 'a') && test(2, 'l') && test(3, 's') && test(4, 'e'))) {
      position = position + 5
      false
    } else {
      throw new ParseException(s"Boolean not found at $position", position)
    }
  }

  private def nextIntegral[T] (toIntegral: String => T): T = {
    var startPosition = position
    if (position < length) {
      var c = raw.charAt(position)
      // Sign
      if (c == '-') { c = advance() }
      // Absolute value
      c = nextNonDigit()
    }

    if (startPosition == position) { throw new ParseException(s"No integer at position $position", position) }
    toIntegral(raw.substring(startPosition, position))
  }

  /**
    * Greedily parse and advance past an integer that must be found at the current position
    *
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the number
    * @return The next number, as an integer
    */
  def nextInt (eatInitialWhitespace: Boolean = true): Int = {
    if (eatInitialWhitespace) { eatWhitespace() }
    nextIntegral(_.toInt)
  }

  /**
    * Greedily parse and advance past a long that must be found at the current position
    *
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the number
    * @return the next number, as a long
    */
  def nextLong (eatInitialWhitespace: Boolean = true): Long = {
    if (eatInitialWhitespace) { eatWhitespace() }
    nextIntegral(_.toLong)
  }

  private def nextFractional[T] (toFractional: String => T): T = {
    val startPosition = position
    if (position < length) {
      var c = raw.charAt(position)
      // Sign
      if (c == '-') { c = advance() }
      // Mantissa
      // Integer portion
      c = nextNonDigit()
      // decimal place
      if ('.' == c) {
        c = advance()
        // fractional part
        c = nextNonDigit()
      }
      // Radix
      if ('e' == c || 'E' == c && length > position + 1) {
        val c1 = raw.charAt(position + 1)
        if ('0' <= c1 && c1 <= '9') {
          // Have a positive radix. Find the end of it.
          c = advance()
          c = nextNonDigit()
        } else if ('-' == c1 && length > position + 2) {
          val c2 = raw.charAt(position + 2)
          if ('0' <= c2 && c2 <= '9') {
            // Have a negative radix. Find the end of it.
            c = advance()
            c = advance()
            c = nextNonDigit()
          }
        }
      }
    }

    toFractional(raw.substring(startPosition, position))
  }

  /**
    * Greedily parse and advance past a float that must be found at the current position
    *
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the number
    * @return the next number, as a float
    */
  def nextFloat (eatInitialWhitespace: Boolean = true): Float = {
    if (eatInitialWhitespace) { eatWhitespace() }
    nextFractional(_.toFloat)
  }

  /**
    * Greedily parse and advance past a double that must be found at the current position
    *
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the number
    * @return the next number, as a double
    */
  def nextDouble (eatInitialWhitespace: Boolean = true): Double = {
    if (eatInitialWhitespace) { eatWhitespace() }
    nextFractional(_.toDouble)
  }

  /**
    * Parse and advance past a quoted, escapeed string in the text
    *
    * @param eatInitialWhitespace If true, skip any whitespace found before looking for the string
    * @return The string found, unquoted and unescaped.
    */
  def nextString (eatInitialWhitespace: Boolean = true): String = {
    if (eatInitialWhitespace) { eatWhitespace() }

    if (position >= length) { null }
    else if (raw.startsWith("null", position)) {
      position += 4
      null
    } else {
      var c = raw.charAt(position)
      if ('\"' != c) {
        throw new ParseException(s"No quote to begin quoted string at $position", position)
      }
      c = advance()
      val startPosition = position
      if ('\\' == c) {
        c = advance()
        c = advance()
      }

      while ('\"' != c && position < length) {
        c = advance()
        if ('\\' == c) {
          c = advance()
          c = advance()
        }
      }

      if ('\"' != c) {
        throw new ParseException(s"No end quote found for quoted substring beginning at $startPosition", position)
      }
      val endPosition = position
      advance()

      raw.substring(startPosition, endPosition).replace("\\\"", "\"").replace("\\\\", "\\")
    }
  }

  /**
    * Read a sequence of items
    *
    * @param parseT A function to extract the next item of type T
    * @param start A start string expected to begin the sequence
    * @param separator A separator string expected between members of the sequence
    * @param end An end string expected at the end of the sequence
    * @param eatAllWhitespace If true, skip any whitespace found before and between any tokens in the list
    * @tparam T The type of object expected and returned
    * @return A sequence of items found
    */
  def nextSeq[T] (parseT: () => T, start: Option[String], separator: Option[String], end: Option[String], eatAllWhitespace: Boolean = true): Seq[T] = {

    start.map(s => eat(s))

    def atEnd: Boolean = {
      if (position == length) { true }
      else if (eatAllWhitespace) {
        var tempPosition = position
        while (tempPosition < length && isWhitespace(raw.charAt(tempPosition))) tempPosition += 1
        tempPosition == length
      } else { false }
    }

    val results = MutableBuffer[T]()
    var done = false

    while (!done && !end.map(e => startsWith(e, eatAllWhitespace)).getOrElse(false) && !atEnd) {
      results += parseT()
      separator.map(s =>
        if (startsWith(s, eatAllWhitespace)) { eat(s, eatAllWhitespace) }
        else { done = true }
      )
    }
    end.map(e => eat(e, eatAllWhitespace))

    results
  }
}
object StringParser {
  /**
    * Escape a string in a manner that is exactly opposite how the parser will try to unescape it when parsing.
    *
    * @param string
    * @return
    */
  def escapeString (string: String): String = {
    if (null == string) { "null" }
    else { "\"" + string.replace("\\", "\\\\").replace("\"", "\\\"") + "\"" }
  }
}
//scalastyle:on null multiple.string.literals cyclomatic.complexity
