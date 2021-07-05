/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl.parse
import scala.util.parsing.combinator._

class BstParser extends RegexParsers {
  def word: Parser[String]    = """[a-z]+""".r ^^ { _.toString }
  def number: Parser[Int]    = """(0|[1-9]\d*)""".r ^^ { _.toInt }

}

