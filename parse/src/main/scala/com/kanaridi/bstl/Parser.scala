/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl

import java.lang.{ StringBuilder => JStringBuilder }

import scala.util.parsing.combinator.RegexParsers

import com.kanaridi.bstl.AST._

class StringBuilderPool extends ThreadLocal[JStringBuilder] {

  override def initialValue() = new JStringBuilder(512)

  override def get(): JStringBuilder = {
    val sb = super.get()
    sb.setLength(0)
    sb
  }
}

object FastStringOps {

  private val stringBuilderPool = new StringBuilderPool

  implicit class RichString(val text: String) extends AnyVal {
    def fastReplaceAll(replaced: String, replacement: String): String =
      if (replaced.isEmpty || replacement.isEmpty) {
        text
      } else {
        var end = text.indexOf(replaced)
        if (end == -1) {
          text
        } else {
          var start = 0
          val replacedLength = replaced.length
          val buf = stringBuilderPool.get()
          while (end != -1) {
            buf.append(text, start, end).append(replacement)
            start = end + replacedLength
            end = text.indexOf(replaced, start)
          }
          buf.append(text, start, text.length).toString
        }
      }
  }
}

object Parser extends RegexParsers {

  private val NumberRegex = """-?\d+""".r
  private val FieldRegex = """[^\*\.\[\]\(\)\,=!<>\s]+""".r
  private val SingleQuotedFieldRegex = """(\\.|[^'])+""".r
  private val DoubleQuotedFieldRegex = """(\\.|[^"])+""".r
  private val SingleQuotedValueRegex = """(\\.|[^'])*""".r
  private val DoubleQuotedValueRegex = """(\\.|[^"])*""".r
  private val NumberValueRegex = """-?\d+(\.\d*)?""".r

  /// general purpose parsers ///////////////////////////////////////////////

  private def number: Parser[Int] = NumberRegex ^^ (_.toInt)

  private def field: Parser[String] = FieldRegex
  private def nameToken: Parser[Name] = FieldRegex ^^ Name

  ///
  private def bstlNameToken: Parser[Name] = FieldRegex ^^ Name
  /// 


  import FastStringOps._
  private def singleQuotedField = "'" ~> SingleQuotedFieldRegex <~ "'" ^^ (_.fastReplaceAll("\\'", "'"))
  private def doubleQuotedField = "\"" ~> DoubleQuotedFieldRegex <~ "\"" ^^ (_.fastReplaceAll("\\\"", "\""))
  private def singleQuotedValue = "'" ~> SingleQuotedValueRegex <~ "'" ^^ (_.fastReplaceAll("\\'", "'"))
  private def doubleQuotedValue = "\"" ~> DoubleQuotedValueRegex <~ "\"" ^^ (_.fastReplaceAll("\\\"", "\""))
  private def quotedField: Parser[String] = singleQuotedField | doubleQuotedField
  private def quotedValue: Parser[String] = singleQuotedValue | doubleQuotedValue

  /// array parsers /////////////////////////////////////////////////////////

  private def arraySliceStep: Parser[Option[Int]] = ":" ~> number.?

  private def arraySlice: Parser[ArraySlice] =
    (":" ~> number.?) ~ arraySliceStep.? ^^ {
      case end ~ step => ArraySlice(None, end, step.flatten.getOrElse(1))
    }

  private def arrayRandomAccess: Parser[Option[ArrayRandomAccess]] =
    rep1("," ~> number).? ^^ (_.map(ArrayRandomAccess))

  private def arraySlicePartial: Parser[ArrayAccessor] =
    number ~ arraySlice ^^ {
      case i ~ as => as.copy(start = Some(i))
    }

  private def arrayRandomAccessPartial: Parser[ArrayAccessor] =
    number ~ arrayRandomAccess ^^ {
      case i ~ None                             => ArrayRandomAccess(i :: Nil)
      case i ~ Some(ArrayRandomAccess(indices)) => ArrayRandomAccess(i :: indices)
    }

  private def arrayPartial: Parser[ArrayAccessor] =
    arraySlicePartial | arrayRandomAccessPartial

  private def arrayAll: Parser[ArraySlice] =
    "*" ^^^ ArraySlice.All

  private[bstl] def arrayAccessors: Parser[ArrayAccessor] =
    "[" ~> (arrayAll | arrayPartial | arraySlice) <~ "]"

  /// filters parsers ///////////////////////////////////////////////////////

  private def numberValue: Parser[JPNumber] = NumberValueRegex ^^ {
    s => if (s.indexOf('.') != -1) JPDouble(s.toDouble) else JPLong(s.toLong)
  }

  private def booleanValue: Parser[FilterDirectValue] =
    "true" ^^^ JPTrue |
      "false" ^^^ JPFalse


  private def nullValue: Parser[FilterValue] =
    "null" ^^^ JPNull

  private def stringValue: Parser[JPString] = quotedValue ^^ { JPString }
  private def value: Parser[FilterValue] = booleanValue | numberValue | nullValue | stringValue

  ////

  private def bstlNumberValue: Parser[NumberExpr] = NumberValueRegex ^^ {
    s => if (s.indexOf('.') != -1) DoubleExpr(s.toDouble) else LongExpr(s.toLong)
  }

  private def bstlNow: Parser[BstlExpression] =
    "now()" ^^^ Now

  private def bstlBooleanValue: Parser[BooleanExpr] =
    "true" ^^^ BooleanExpr(true) |
  "false" ^^^ BooleanExpr(false)

  private def bstlStringValue: Parser[StringExpr] = quotedValue ^^ {
    StringExpr
  }

  private def bstlValue: Parser[VariantExpr] = bstlBooleanValue | bstlNumberValue | bstlStringValue 

  private def toUpper: Parser[BstlExpression] =
    "toUpper(" ~> bstlExpression <~ ")" ^^ {
      x => ToUpper(x)
    }

  private def toLower: Parser[BstlExpression] =
    "toLower(" ~> bstlExpression <~ ")" ^^ {
      x => ToLower(x)
    }

  private def concat: Parser[BstlExpression] =
    "concat(" ~> bstlExpression ~ "," ~ bstlExpression <~ ")" ^^ {
      case lhs ~ comma ~ rhs => Concat(lhs, rhs)
    }

  private def trim: Parser[BstlExpression] =
    "trim(" ~> bstlExpression <~ ")" ^^ {
      x => Trim(x)
    }

  private def matches: Parser[BstlExpression] =
    "matches(" ~> bstlExpression ~ "," ~ bstlExpression <~ ")" ^^ {
      case lhs ~ comma ~ rhs => Matches(lhs, rhs)
    }

  private def subString: Parser[BstlExpression] =
    "subString(" ~> bstlExpression ~ "," ~ bstlExpression ~ "," ~ bstlExpression <~ ")" ^^ {
      case src ~ comma1 ~ start ~ comma2 ~ end => SubString(src, start, end)
    }

  private def stringExpression: Parser[BstlExpression] =
    toUpper | toLower | concat | trim | subString 

  ////


  private def numberExpression: Parser[BstlExpression]  = bstlNow

  private def comparisonOperator: Parser[ComparisonOperator] =
    "==" ^^^ EqOperator |
      "!=" ^^^ NotEqOperator |
      "<=" ^^^ LessOrEqOperator |
      "<" ^^^ LessOperator |
      ">=" ^^^ GreaterOrEqOperator |
      ">" ^^^ GreaterOperator

  private def current: Parser[PathToken] = "@" ^^^ CurrentNode

  private def subQuery: Parser[SubQuery] =
    (current | root) ~ pathSequence ^^ { case c ~ ps => SubQuery(c :: ps) }

  private def expression1: Parser[FilterToken] =
    subQuery ~ (comparisonOperator ~ (subQuery | value)).? ^^ {
      case subq1 ~ None         => HasFilter(subq1)
      case lhs ~ Some(op ~ rhs) => ComparisonFilter(op, lhs, rhs)
    }

  private def expression2: Parser[FilterToken] =
    value ~ comparisonOperator ~ subQuery ^^ {
      case lhs ~ op ~ rhs => ComparisonFilter(op, lhs, rhs)
    }

  private def expression: Parser[FilterToken] = expression1 | expression2

  private def booleanOperator: Parser[BinaryBooleanOperator] = "&&" ^^^ AndOperator | "||" ^^^ OrOperator

  private def booleanExpression: Parser[FilterToken] =
    expression ~ (booleanOperator ~ booleanExpression).? ^^ {
      case lhs ~ None => lhs
      // Balance the AST tree so that all "Or" operations are always on top of any "And" operation.
      // Indeed, the "And" operations have a higher priority and must be executed first.
      case lhs1 ~ Some(AndOperator ~ BooleanFilter(OrOperator, lhs2, rhs2)) =>
        BooleanFilter(OrOperator, BooleanFilter(AndOperator, lhs1, lhs2), rhs2)
      case lhs ~ Some(op ~ rhs) => BooleanFilter(op, lhs, rhs)
    }

  private def recursiveSubscriptFilter: Parser[RecursiveFilterToken] =
    (("..*" | "..") ~> subscriptFilter) ^^ RecursiveFilterToken

  private[bstl] def subscriptFilter: Parser[FilterToken] =
    "[?(" ~> booleanExpression <~ ")]"

  /// child accessors parsers ///////////////////////////////////////////////

  private[bstl] def subscriptField: Parser[FieldAccessor] =
    "[" ~> repsep(quotedField, ",") <~ "]" ^^ {
      case f1 :: Nil => Field(f1)
      case fields    => MultiField(fields)
    }

  private[bstl] def dotField: Parser[FieldAccessor] =
    "." ~> field ^^ Field

  private[bstl] def literalField: Parser[FieldAccessor] =
    quotedValue ^^ FieldLiteral

  private[bstl] def bstlLiteralField: Parser[BstlExpression] =
    quotedValue ^^ StringExpr

  // TODO recursive with `subscriptField`
  private def recursiveField: Parser[FieldAccessor] =
    ".." ~> field ^^ RecursiveField

  private def anyChild: Parser[FieldAccessor] = (".*" | "['*']" | """["*"]""") ^^^ AnyField

  private def recursiveAny: Parser[FieldAccessor] = "..*" ^^^ RecursiveAnyField

  private[bstl] def fieldAccessors = (
    dotField
    | recursiveSubscriptFilter
    | recursiveAny
    | recursiveField
    | anyChild
    | subscriptField
  )

  /// Main parsers //////////////////////////////////////////////////////////

  private def childAccess = fieldAccessors | arrayAccessors

  private[bstl] def pathSequence: Parser[List[PathToken]] = rep(childAccess | subscriptFilter)

  private[bstl] def grandparent: Parser[PathToken] = "../../$" ^^^ GrandParentNode
  private[bstl] def parent: Parser[PathToken] = "../$" ^^^ ParentNode
  private[bstl] def root: Parser[PathToken] = "$" ^^^ RootNode

  private def startNode: Parser[PathToken] =
    grandparent |
  parent |
  root

  ///////////////////////////////////////////////////////
  // BSTL

  private def jpQuery: Parser[JsonQuery] =
    (startNode ~ pathSequence) ^^ {
      case r ~ ps => {

        JsonQuery(r :: ps)
      }
    }

  private def infixOperator: Parser[InfixOperator] =
    "+" ^^^ BstlPlus |
      "*" ^^^ BstlTimes |
      "/" ^^^ BstlDiv |
      "%" ^^^ BstlMod |
      "-" ^^^ BstlMinus

  private def mathFunction: Parser[BstlExpression] =
    "round(" ~> bstlExpression <~ ")" ^^ {
      x => BstlRoundExpression(x)
    }


  private def topHli: Parser[BstlExpression] =
    "topHli(" ~> bstlExpression <~ ")" ^^ {
      x => S4Hli(x)
    }

  private def argsList: Parser[List[BstlExpression]] =
    "(" ~> repsep(bstlExpression, ",") <~ ")"

  private def joinFunction: Parser[BstlExpression] = 
    "join(" ~> repsep(bstlExpression, ",") <~ ")" ^^ {
      args => {
        JoinCall( args)
      }
    }

  private def javascriptFunction: Parser[BstlExpression] = 
    "javascript(" ~> repsep(bstlExpression, ",") <~ ")" ^^ {
      args => {
        JavaScriptCall( args)
      }
    }

  private def funCall: Parser[BstlExpression] =
    bstlNameToken ~ argsList ^^ {
      case nm ~ args => {
        println("funCall")
        FunctionCall(nm.name, args)
      }
    }

  private def bstlBooleanCombiner: Parser[BstlExpression] =
    "(" ~> bstlBooleanExpression ~ booleanOperator ~ bstlBooleanExpression <~ ")" ^^ {
      case lhs ~ AndOperator ~ rhs => BstlAndExpression(lhs, rhs)
      case lhs ~ OrOperator ~ rhs => BstlOrExpression(lhs, rhs)
    }

  private def bstlComparisonExpression: Parser[BstlExpression] =
    "(" ~> bstlExpression ~ comparisonOperator ~ bstlExpression <~ ")" ^^ {
      case lhs ~ op ~ rhs => BstlComparison(op, lhs, rhs)
    }

  private def bstlBooleanExpression: Parser[BstlExpression] =
    bstlBooleanValue | bstlNot | bstlComparisonExpression | bstlBooleanCombiner | matches

  private def bstlCompoundExpr: Parser[BstlExpression] =
    "(" ~> bstlExpression ~ (infixOperator ~ bstlQuery).? <~ ")" ^^ {
      case lhs ~ None => lhs
      case lhs ~ Some(op ~ rhs) => BstlInfixExpression(op, lhs, rhs)
    }

  private def bstlNot: Parser[BstlExpression] =
    "not" ~> ( bstlBooleanCombiner | bstlComparisonExpression | bstlCompoundExpr) ^^ {
      x => BstlNot(x)
    }

  private[bstl] def ternary: Parser[BstlExpression ] =
    "if" ~> bstlExpression ~ "then" ~ bstlExpression ~ "else" ~ bstlExpression ^^ {
      case pred ~ x ~ lhs ~ y ~ rhs => BstlIfElse(pred, lhs, rhs) 
    }

  private def bstlExpression: Parser[BstlExpression] =
    ( jpQuery | stringExpression | bstlNow | bstlLiteralField | bstlValue |  ternary | mathFunction | bstlCompoundExpr | bstlBooleanExpression | javascriptFunction | joinFunction | topHli | funCall | bstlNameToken)

  private def bstlQuery: Parser[BstlExpression] =
    ( stringExpression | bstlNow | bstlLiteralField | bstlValue |  ternary | mathFunction | bstlCompoundExpr | bstlBooleanExpression |  javascriptFunction | joinFunction | topHli | funCall | bstlNameToken)

  // the parser root is a list of PathTokens
  private def query: Parser[BstlExpression] = (jpQuery | bstlQuery)
}

class Parser {
  private val query = Parser.query
  def compile(jsonpath: String): Parser.ParseResult[BstlExpression] = Parser.parse(query, jsonpath)
}
