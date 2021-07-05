/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl

import java.util.{ List => JList, Map => JMap }
import java.util.function.Supplier
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.immutable.Map
import scala.math.abs

import com.kanaridi.bstl.AST._

case class JPError(reason: String)

/** */
object JsonPath {
  def parserSupplier() = {
    new Supplier[Parser] {
      override def get: Parser = {
        new Parser
      }
    }
  }

  val factory = new javax.script.ScriptEngineManager
  val javascript = factory.getEngineByName("JavaScript")

  val initialContext = Map[String, Any]("javascript" -> javascript)

  private val JsonPathParser = ThreadLocal.withInitial[Parser](parserSupplier())
  //  private val JsonPathParser = ThreadLocal.withInitial[Parser](() => new Parser)

  def compile(query: String): Either[JPError, JsonPath] =
    JsonPathParser.get.compile(query) match {
      case Parser.Success(q, _) => Right(new JsonPath(q))
      case ns: Parser.NoSuccess => Left(JPError(ns.msg))
    }

  def query(query: String, pobj: Any):  Either[JPError, Iterator[Any]] =
    compile(query).right.map(_.query(initialContext, List(pobj)))

  def query(query: String, jsonObjects: List[Any]): Either[JPError, Iterator[Any]] =
    compile(query).right.map(_.query(initialContext, jsonObjects))

  def eval(query: String, context: Map[String, Any], jsonObjects: List[Any]): Either[JPError, Iterator[Any]] =
    compile(query).right.map(_.query(context, jsonObjects))
}

/** */
class JsonPath(ast: BstlExpression) {
  def query(context: Map[String, Any], jsonObjects: List[Any]): Iterator[Any] = {
    new JsonPathWalker(jsonObjects, ast).eval(context)
  }
}

/** */
class JsonPathWalker(rootNodes: List[Any], expr: BstlExpression) {

  def eval(context: Map[String, Any]): Iterator[Any] = eval(context, rootNodes.head, expr)

  private[this] def eval(context: Map[String, Any], node: Any, expr: BstlExpression): Iterator[Any] = {

    expr match {

      case jq: JsonQuery => {
        walk(node, jq.query)
      }

      // WDL
      case FieldLiteral(value) => {
        Iterator.single(value)
      }

      case fv: FilterDirectValue => {
        Iterator.single(fv.value)
      }

      case fv: Name => {
        Iterator.single(fv.eval(context, rootNodes).convertToString match {
          case Some(x) => x
          case None => ""
        })
      }
      case fv: BstlExpression  => {
        Iterator.single(fv.eval(context, rootNodes).convertToString match {
          case Some(x) => x
          case None => ""
        })
      }

      case _   => Iterator.single("?")

    }
  }

  private[this] def walk(node: Any, path: List[PathToken]): Iterator[Any] =
    path match {
      case head :: tail => walk1(node, head).flatMap(walk(_, tail))
      case _            => Iterator.single(node)
    }

  private[this] def walk1(node: Any, query: PathToken): Iterator[Any] =
    query match {
      case RootNode    => Iterator.single(rootNodes(0))
      case ParentNode    => Iterator.single(rootNodes(1))
      case GrandParentNode    => Iterator.single(rootNodes(2))

      case CurrentNode => Iterator.single(node)

      case Field(name) => node match {
        case obj: JMap[_, _] if obj.containsKey(name) => Iterator.single(obj.get(name))
        case mobj: Map[String, _] @unchecked if mobj.contains(name) => Iterator.single(mobj.getOrElse(name, None))
            // .map _ )
        case _                                        => Iterator.empty
      }

      case RecursiveField(name) => recFieldFilter(node, name)

      case MultiField(fieldNames) => node match {
        case obj: JMap[_, _] =>
          // don't use collect on iterator with filter causes (executed twice)
          fieldNames.iterator.filter(obj.containsKey).map(obj.get)
        case obj: Map[String, _] @unchecked =>
          // don't use collect on iterator with filter causes (executed twice)
          fieldNames.iterator.filter(obj.contains).map(obj.get)
        case _ => Iterator.empty
      }

      case AnyField => node match {
        case obj: JMap[_, _] => obj.values.iterator.asScala
        case obj: Map[_, _] => obj.values.iterator
        case _               => Iterator.empty
      }

      case ArraySlice(None, None, 1) => node match {
        case array: JList[_] => array.asScala.iterator
        case array: List[_] => array.iterator
        case _               => Iterator.empty
      }

      case ArraySlice(start, stop, step) => node match {
        // FIXME
        case array: JList[_] => sliceArray(array, start, stop, step)
        case array: List[_] => sliceArray(array, start, stop, step)
        case _               => Iterator.empty
      }

      case ArrayRandomAccess(indices) => node match {
        case array: JList[_] => indices.iterator.collect {
          case i if i >= 0 && i < array.size  => array.get(i)
          case i if i < 0 && i >= -array.size => array.get(i + array.size)
        }
        case array: List[_] => indices.iterator.collect {
          case i if i >= 0 && i < array.length  => array(i)
          case i if i < 0 && i >= -array.length => array(i + array.size)
        }
        case _ => Iterator.empty
      }

      case RecursiveFilterToken(filterToken) => recFilter(node, filterToken)

      case filterToken: FilterToken          => applyFilter(node, filterToken)

      case RecursiveAnyField                 => Iterator.single(node) ++ recFieldExplorer(node)
      case _   => Iterator.empty
    }

  private[this] def recFilter(node: Any, filterToken: FilterToken): Iterator[Any] = {

    def allNodes(curr: Any): Iterator[Any] = curr match {
      case array: JList[_]                 => array.asScala.iterator.flatMap(allNodes)
      case array: List[_]                 => array.iterator.flatMap(allNodes)
      case obj: JMap[_, _] if !obj.isEmpty => Iterator.single(obj) ++ obj.values.iterator.asScala.flatMap(allNodes)
      case obj: Map[_, _] if !obj.isEmpty => Iterator.single(obj) ++ obj.values.iterator.flatMap(allNodes)
      case _                               => Iterator.empty
    }

    allNodes(node).flatMap(applyFilter(_, filterToken))
  }

  private[this] def applyFilter(currentNode: Any, filterToken: FilterToken): Iterator[Any] = {

    def resolveSubQuery(node: Any, q: List[AST.PathToken], nextOp: Any => Boolean): Boolean = {
      val it = walk(node, q)
      it.hasNext && nextOp(it.next())
    }

    def applyBinaryOpWithResolvedLeft(node: Any, op: ComparisonOperator, lhsNode: Any, rhs: FilterValue): Boolean =
      rhs match {
        case direct: FilterDirectValue => op(lhsNode, direct.value)
        case SubQuery(q)               => resolveSubQuery(node, q, op(lhsNode, _))
      }

    def applyBinaryOp(node: Any, op: ComparisonOperator, lhs: FilterValue, rhs: FilterValue): Boolean =
      lhs match {
        case direct: FilterDirectValue => applyBinaryOpWithResolvedLeft(node, op, direct.value, rhs)
        case SubQuery(q)               => resolveSubQuery(node, q, applyBinaryOpWithResolvedLeft(node, op, _, rhs))
      }

    def elementsToFilter(node: Any): Iterator[Any] =
      node match {
        case array: JList[_] => array.asScala.iterator
        case array: List[_] => array.iterator
        case obj: JMap[_, _] => Iterator.single(obj)
        case obj: Map[_, _] => Iterator.single(obj)
        case _               => Iterator.empty
      }

    def evaluateFilter(filterToken: FilterToken): Any => Boolean =
      filterToken match {
        case HasFilter(subQuery) =>
          (node: Any) => walk(node, subQuery.path).hasNext

        case ComparisonFilter(op, lhs, rhs) =>
          (node: Any) => applyBinaryOp(node, op, lhs, rhs)

        case BooleanFilter(op, filter1, filter2) =>
          val f1 = evaluateFilter(filter1)
          val f2 = evaluateFilter(filter2)
            (node: Any) => op(f1(node), f2(node))
      }

    val filterFunction = evaluateFilter(filterToken)
    elementsToFilter(currentNode).filter(filterFunction)
  }

  def recFieldFilter(node: Any, name: String): Iterator[Any] = {
    def recFieldFilter0(node: Any): Iterator[Any] =
      node match {
        case obj: JMap[_, _] =>
          obj.entrySet.iterator.asScala.flatMap(e => e.getKey match {
            case `name` => Iterator.single(e.getValue)
            case _      => recFieldFilter0(e.getValue)
          })
        case list: JList[_] => list.iterator.asScala.flatMap(recFieldFilter0)
        case _              => Iterator.empty
      }

    recFieldFilter0(node)
  }

  def recFieldExplorer(node: Any): Iterator[Any] =
    node match {
      case obj: JMap[_, _] =>
        val values = obj.values
        values.iterator.asScala ++ values.iterator.asScala.flatMap(recFieldExplorer)
      case list: JList[_] =>
        list.iterator.asScala.flatMap(recFieldExplorer)
      case _ => Iterator.empty
    }

  private[this] def sliceArray(array: JList[_], start: Option[Int], stop: Option[Int], step: Int): Iterator[Any] = {
    val size = array.size

    def lenRelative(x: Int) = if (x >= 0) x else size + x
    def stepRelative(x: Int) = if (step >= 0) x else -1 - x
    def relative(x: Int) = lenRelative(stepRelative(x))

    val absStart = start match {
      case Some(v) => relative(v)
      case _       => 0
    }
    val absEnd = stop match {
      case Some(v) => relative(v)
      case _       => size
    }
    val absStep = abs(step)

    val elements: Iterator[Any] = if (step < 0) array.asScala.reverseIterator else array.asScala.iterator
    val fromStartToEnd = elements.slice(absStart, absEnd)

    if (absStep == 1)
      fromStartToEnd
    else
      fromStartToEnd.grouped(absStep).map(_.head)
  }

  private[this] def sliceArray(array: List[_], start: Option[Int], stop: Option[Int], step: Int): Iterator[Any] = {
    val size = array.size

    def lenRelative(x: Int) = if (x >= 0) x else size + x
    def stepRelative(x: Int) = if (step >= 0) x else -1 - x
    def relative(x: Int) = lenRelative(stepRelative(x))

    val absStart = start match {
      case Some(v) => relative(v)
      case _       => 0
    }
    val absEnd = stop match {
      case Some(v) => relative(v)
      case _       => size
    }
    val absStep = abs(step)

    val elements: Iterator[Any] = if (step < 0) array.reverseIterator else array.iterator
    val fromStartToEnd = elements.slice(absStart, absEnd)

    if (absStep == 1)
      fromStartToEnd
    else
      fromStartToEnd.grouped(absStep).map(_.head)
  }
}
