/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl.parse

import scala.util.parsing.combinator._

sealed trait BstToken

case class IDENTIFIER(str: String) extends BstToken
case class LITERAL(str: String) extends BstToken
case class INT(int: Int)  extends BstToken

case object CHOOSE extends BstToken
case object WHEN extends BstToken
case object OTHERWISE extends BstToken
case object COLON extends BstToken
case object ARROW extends BstToken
case object NOT extends BstToken
case object EQUALS extends BstToken
case object GREATER extends BstToken
case object LESS extends BstToken
case object NOTEQUAL extends BstToken
case object GREATEREQUALS extends BstToken
case object LESSEQUALS extends BstToken
case object OPENPAREN extends BstToken
case object CLOSEPAREN extends BstToken
case object COMMA extends BstToken

trait BstCompilationError
case class BstLexerError(msg: String) extends BstCompilationError

object BstLexer extends RegexParsers {

  def identifier: Parser[IDENTIFIER] = {
    "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => IDENTIFIER(str) }
  }

  def number: Parser[INT]    = """(0|[1-9]\d*)""".r ^^ {str => INT(str.toInt) }

  def literal: Parser[LITERAL] = {
    """"[^"]*"""".r ^^ { str =>
      val content = str.substring(1, str.length - 1)
      LITERAL(content)
    }
  }

  def choose        = "choose"        ^^ (_ => CHOOSE)
  def when          = "when"          ^^ (_ => WHEN)
  def otherwise     = "otherwise"     ^^ (_ => OTHERWISE)
  def colon         = ":"             ^^ (_ => COLON)
  def arrow         = "->"            ^^ (_ => ARROW)
  def equals        = "=="            ^^ (_ => EQUALS)
  def notequals     = "!="            ^^ (_ => EQUALS)
  def comma         = ","             ^^ (_ => COMMA)
  def openparen     = "("             ^^ (_ => OPENPAREN)
  def closeparen    = ")"             ^^ (_ => CLOSEPAREN)

  def tokens: Parser[List[BstToken]] = {
    phrase(rep1(choose | otherwise | colon | arrow
      | equals | notequals | comma | literal | identifier | number | openparen | closeparen )) ^^ { rawTokens =>
      rawTokens
    }
  }

  def apply(code: String): Either[BstLexerError, List[BstToken]] = {
    parse(tokens, code) match {
      case NoSuccess(msg, next) => Left(BstLexerError(msg))
      case Success(result, next) => Right(result)
    }
  }
}

