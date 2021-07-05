/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl

/** Abstract Syntax Tree
  */
object AST {
  sealed trait AstToken
  // BSTL AST

  sealed trait BstlExpression extends AstToken {
    def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      BstlFalse
    }
  }

  sealed trait PathToken extends BstlExpression

  sealed trait FieldAccessor extends PathToken

  sealed trait VariantExpr extends BstlExpression {
    def isNumber(): Boolean = { false }
    def isDouble(): Boolean = { false }
    def isLong(): Boolean = { false }
    def isString(): Boolean = { false }
    def isBoolean(): Boolean = { false }
    def isMap(): Boolean = { false }
    def isArray(): Boolean = { false }
  }

  sealed trait NumberExpr extends VariantExpr

  case class DoubleExpr(value: Double) extends NumberExpr {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      ConvertibleDoubleVariant(value)
    }

    override def isNumber() = { true }
    override def isDouble() = { true }
  }

  case class LongExpr(value: Long) extends NumberExpr {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      ConvertibleLongVariant(value)
    }

    override def isNumber() = { true }
    override def isLong() = { true }
  }

  case class BooleanExpr(value: Boolean) extends VariantExpr {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      if ( value ) BstlTrue else BstlFalse
    }

    override def isBoolean() = { true }
  }

  case class StringExpr(value: String) extends VariantExpr {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      ConvertibleStringVariant(value)
    }

    override def isString() = { true }
  }

  ////////////////////////

  sealed trait InfixOperator extends AstToken {
    def op(lhs: ConvertibleVariant, rhs: ConvertibleVariant) : ConvertibleVariant = {
      ConvertibleLongVariant(0)
    }
  }

  object BstlPlus extends InfixOperator {
    override def op(lhs: ConvertibleVariant, rhs: ConvertibleVariant) : ConvertibleVariant = {
      if (lhs.isDouble || rhs.isDouble) {
        ConvertibleDoubleVariant(lhs.convertToDouble.getOrElse(0.0) + rhs.convertToDouble.getOrElse(0.0))
      } else {
        val s: Long = lhs.convertToLong.getOrElse(0)
        val r: Long = rhs.convertToLong.getOrElse(0)
        ConvertibleLongVariant(s + r)
        // ConvertibleLongVariant(lhs.convertToLong.getOrElse(0) + rhs.convertToLong.getOrElse(0))
      }
    }
  }

  object BstlMinus extends InfixOperator {
    override def op(lhs: ConvertibleVariant, rhs: ConvertibleVariant) : ConvertibleVariant = {
      if (lhs.isDouble || rhs.isDouble) {
        ConvertibleDoubleVariant(lhs.convertToDouble.getOrElse(0.0) - rhs.convertToDouble.getOrElse(0.0))
      } else {
        val s: Long = lhs.convertToLong.getOrElse(0)
        val r: Long = rhs.convertToLong.getOrElse(0)
        ConvertibleLongVariant(s - r)
        // ConvertibleLongVariant(lhs.convertToLong.getOrElse(0) + rhs.convertToLong.getOrElse(0))
      }
    }
  }

  object BstlTimes extends InfixOperator  {
    override def op(lhs: ConvertibleVariant, rhs: ConvertibleVariant) : ConvertibleVariant = {
      if (lhs.isDouble || rhs.isDouble) {
        ConvertibleDoubleVariant(lhs.convertToDouble.getOrElse(0.0) * rhs.convertToDouble.getOrElse(0.0))
      } else {
        val s: Long = lhs.convertToLong.getOrElse(0)
        val r: Long = rhs.convertToLong.getOrElse(0)
        ConvertibleLongVariant(s * r)
        // ConvertibleLongVariant(lhs.convertToLong.getOrElse(0) + rhs.convertToLong.getOrElse(0))
      }
    }
  }

  object BstlDiv extends InfixOperator   {
    override def op(lhs: ConvertibleVariant, rhs: ConvertibleVariant) : ConvertibleVariant = {
      if (lhs.isDouble || rhs.isDouble) {
        ConvertibleDoubleVariant(lhs.convertToDouble.getOrElse(0.0) / rhs.convertToDouble.getOrElse(0.0))
      } else {
        val s: Long = lhs.convertToLong.getOrElse(0)
        val r: Long = rhs.convertToLong.getOrElse(0)
        ConvertibleLongVariant(s / r)
        // ConvertibleLongVariant(lhs.convertToLong.getOrElse(0) + rhs.convertToLong.getOrElse(0))
      }
    }
  }

  object BstlMod extends InfixOperator  {
    override def op(lhs: ConvertibleVariant, rhs: ConvertibleVariant) : ConvertibleVariant = {
      if (lhs.isDouble || rhs.isDouble) {
        ConvertibleDoubleVariant(lhs.convertToDouble.getOrElse(0.0) % rhs.convertToDouble.getOrElse(0.0))
      } else {
        val s: Long = lhs.convertToLong.getOrElse(0)
        val r: Long = rhs.convertToLong.getOrElse(0)
        ConvertibleLongVariant(s % r)
        // ConvertibleLongVariant(lhs.convertToLong.getOrElse(0) + rhs.convertToLong.getOrElse(0))
      }
    }
  }

  case class BstlRoundExpression(lhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val v1 = lhs.eval(context, nodes).convertToDouble.getOrElse(0.0).toDouble
      ConvertibleLongVariant(Math.round(v1))
    }
  }

  case class BstlComparison(op: ComparisonOperator, lhs: BstlExpression, rhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleBooleanVariant = {
      val lcv = lhs.eval(context, nodes).convertToString.getOrElse("")
      val rcv = rhs.eval(context, nodes).convertToString.getOrElse("")
      // println(s"comparisonOperator $op for lcv $lcv to $rcv")
      if (op.apply(lcv, rcv)) BstlTrue else BstlFalse
    }
  }

  case class BstlNot(lhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val v1 = lhs.eval(context, nodes).convertToBoolean
      val b1 = v1 match {
        case Some(x) => x
        case None => false
      }
      if (b1) BstlFalse else BstlTrue
    }
  }

  case class BstlAndExpression(lhs: BstlExpression, rhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val v1 = lhs.eval(context, nodes).convertToBoolean
      val b1 = v1 match {
        case Some(x) => x
        case None => false
      }
      val b2 = rhs.eval(context, nodes).convertToBoolean match {
        case Some(x) => x
        case None => false
      }
      if (b1 && b2) BstlTrue else BstlFalse
    }
  }

  case class BstlOrExpression(lhs: BstlExpression, rhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val v1 = lhs.eval(context, nodes).convertToBoolean
      val b1 = v1 match {
        case Some(x) => x
        case None => false
      }
      val b2 = rhs.eval(context, nodes).convertToBoolean match {
        case Some(x) => x
        case None => false
      }
      if (b1 || b2) BstlTrue else BstlFalse
    }
  }

  case class BstlIfElse(condition: BstlExpression, lhs: BstlExpression, rhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val b2 = condition.eval(context, nodes).convertToBoolean match {
        case Some(x) => x
        case None => false
      }

      if ( b2 ) lhs.eval(context, nodes) else rhs.eval(context, nodes)
    }
  }

  object Now extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleLongVariant = {
      ConvertibleLongVariant(System.currentTimeMillis())
    }
  }

  // String functions
  case class ToUpper(from: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val bstr = from.eval(context, nodes).convertToString match {
        case Some(x) => x.toUpperCase
        case None => ""
      }
      ConvertibleStringVariant(bstr)
    }
  }

  case class ToLower(from: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val bstr = from.eval(context, nodes).convertToString match {
        case Some(x) => x.toLowerCase
        case None => ""
      }
      ConvertibleStringVariant(bstr)
    }
  }

  case class Concat(lhs: BstlExpression, rhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val bstr1 = lhs.eval(context, nodes).convertToString match {
        case Some(x) => x
        case None => ""
      }
      val bstr2 = rhs.eval(context, nodes).convertToString match {
        case Some(x) => x
        case None => ""
      }
      ConvertibleStringVariant(bstr1 + bstr2)
    }
  }


  case class MultiConcat(args: List[BstlExpression]) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      ConvertibleStringVariant("foo")
    }
  }

  case class Trim(from: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val bstr1 = from.eval(context, nodes).convertToString match {
        case Some(x) => x.trim
        case None => ""
      }
      ConvertibleStringVariant(bstr1)
    }
  }

  case class Matches(lhs: BstlExpression, regex: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val bstr1 = lhs.eval(context, nodes).convertToString match {
        case Some(x) => x
        case None => ""
      }
      val bstr2 = regex.eval(context, nodes).convertToString match {
        case Some(x) => x
        case None => ""
      }
      if (bstr1.matches(bstr2)) BstlTrue else BstlFalse
    }
  }

  case class SubString(source: BstlExpression, start: BstlExpression, end: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val src1 = source.eval(context, nodes).convertToString match {
        case Some(x) => x
        case None => ""
      }
      val start1 = start.eval(context, nodes).convertToLong match {
        case Some(x) => x.toInt
        case None => 0
      }
      val end1 = end.eval(context, nodes).convertToLong match {
        case Some(x) => x.toInt
        case None => 0
      }
      ConvertibleStringVariant(if (end1 >= start1)
        src1.substring(start1, end1)
      else src1.substring(start1))

    }
  }

  /** custom getter for S4 top HLI */
  case class S4Hli(sapitemnum: BstlExpression) extends BstlExpression {

    def topLi(key: String, jobj: Any): String = {
      // println(s"topLi: key is $key")
      val query = s"$$.*.IDOC.E1EDP01..[?(@.POSEX == '$key')].UEPOS"
      // println(s"topLi: query is $query")
      val qr = JsonPath.query(query, List(jobj))
      val li = qr match {
        case Left(x) => Vector[Any]()
        case Right(x) => {
          val item0 = qr.right.map(_.toVector).right.get
          // println(s"x $x yelds item0 $item0")
          item0
        }
      }
      val hli = li.mkString

      if (hli == "000000" || hli == "") key else topLi(hli, jobj)
    }

    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val lineItemKey = sapitemnum.eval(context, nodes).convertToString match {
        case Some(x) => x
        case None => "000010"
      }
      println(s"getting topLi from $lineItemKey")//  in ${nodes.last}")
      ConvertibleStringVariant(topLi(lineItemKey, nodes.last))
    }
  }


  // Math functions

  case class BstlInfixExpression(op: InfixOperator, lhs: BstlExpression, rhs: BstlExpression) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      op.op(lhs.eval(context, nodes), rhs.eval(context, nodes))
    }
  }

  case object RootNode extends FieldAccessor
  case object ParentNode extends FieldAccessor
  case object GrandParentNode extends FieldAccessor

  case class Field(name: String) extends FieldAccessor

  case class FieldLiteral(value: String) extends FieldAccessor
  case class RecursiveField(name: String) extends FieldAccessor
  case class MultiField(names: List[String]) extends FieldAccessor
  case object AnyField extends FieldAccessor
  case object RecursiveAnyField extends FieldAccessor

  sealed trait ArrayAccessor extends PathToken

  /**
   * Slicing of an array, indices start at zero
   *
   * @param start is the first item that you want (of course)
   * @param stop is the first item that you do not want
   * @param step, being positive or negative, defines whether you are moving
   */
  case class ArraySlice(start: Option[Int], stop: Option[Int], step: Int = 1) extends ArrayAccessor
  object ArraySlice {
    val All = ArraySlice(None, None)
  }
  case class ArrayRandomAccess(indices: List[Int]) extends ArrayAccessor


  case class JsonQuery(query: List[PathToken]) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {

      val cp = new JsonPath(this).query(context, nodes)
      // println(s"query is $query, cp is $cp")
      val cp2 = cp.map( s  => {
        val sv = s.toString
        // println(s"item is $sv")
        sv
      }).mkString
      // println(s"query is $query, cp2 is $cp2")
      ConvertibleStringVariant(cp2)
    }
  }

  case class FunctionCall(name: String, query: List[BstlExpression]) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      ConvertibleStringVariant(s"function-$name")
    }
  }

  case class JoinCall(query: List[BstlExpression]) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val strmap = query.flatMap ( lhs => {
        lhs.eval(context, nodes).convertToString.map(x => {
          x
        })
      })
      val result = strmap.tail.mkString(strmap.head)
      ConvertibleStringVariant(result)
    }
  }

  case class JavaScriptCall(query: List[BstlExpression]) extends BstlExpression {
    override def eval(context: Map[String, Any], nodes: List[Any]) : ConvertibleVariant = {
      val expr = query.head.eval(context, nodes).convertToString.getOrElse("")
      val engine = context.get("javascript")
      val result = engine match {
        case Some(js) => js.asInstanceOf[javax.script.ScriptEngine].eval(expr)
        case None => ""
      }
      ConvertibleStringVariant(result.toString)
    }
  }
  // JsonPath Filter AST //////////////////////////////////////////////

  case object CurrentNode extends PathToken
  sealed trait FilterValue extends PathToken
  sealed trait FilterDirectValue extends FilterValue {
    def value: Any
  }

  /** token representing a string literal */
  case class Name(name: String) extends FieldAccessor {
    override def eval(context: Map[String, Any], nodes: List[Any]): ConvertibleVariant = {
      // println(s"look for $name in the context")
      // lookup value in the context
      val result = context.get(name) match {
        case Some(nameVal) => nameVal.toString
        case None => name
      }
      ConvertibleStringVariant(result)
    }
  }

  sealed trait JPNumber extends FilterDirectValue
  case class JPLong(value: Long) extends JPNumber
  case class JPDouble(value: Double) extends JPNumber
  case object JPTrue extends FilterDirectValue { val value = true }
  case object JPFalse extends FilterDirectValue { val value = false }
  case class JPString(value: String) extends FilterDirectValue
  case object JPNull extends FilterDirectValue { val value = null }

  case class SubQuery(path: List[PathToken]) extends FilterValue

  sealed trait FilterToken extends PathToken
  case class HasFilter(query: SubQuery) extends FilterToken
  case class ComparisonFilter(operator: ComparisonOperator, lhs: FilterValue, rhs: FilterValue) extends FilterToken
  case class BooleanFilter(fun: BinaryBooleanOperator, lhs: FilterToken, rhs: FilterToken) extends FilterToken

  case class RecursiveFilterToken(filter: FilterToken) extends PathToken
}
