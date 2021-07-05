/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl

import scala.reflect.ClassTag
import scala.util.{Success, Try, Failure}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.scalatest._

import org.scalamock.scalatest.MockFactory

class ParserTest extends FunSuite with MockFactory {

  val json = """{
    "myObj": {
       "myTrue": true,
       "myFalse": false,
       "myInt": 42,
       "myFloat": 123.44,
       "myFoo": "foo",
       "myArray": [ "x", "y", "z" ],
       "objArray": [ 
          { "k1": true, "k2": "o1" }, 
          { "k1": false, "k2": "o2" }, 
          { "k1": true, "k2": "o3" },
          { "k1": false, "k2": "o4" },
          { "k1": true, "k2": "o5" } 
       ]
    }
  }"""

  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  val jobj = mapper.readValue(json, classOf[Object])
  println(s"jobj : ${jobj.getClass} is $jobj")

  println(s"jobj as json '${mapper.writeValueAsString(jobj)}'")

  def stringExpr(expr: String) : String = {
    val qr = JsonPath.query(expr, jobj)

    qr match {
      case Left(x) => ""
      case Right(x) => {
        // println(s"x : ${x.getClass} is $x")
        val item0 = qr.right.map(_.toVector).right.get
        // println(s"item0: ${item0.getClass} is $item0")

        item0.map(x => {
          //          println(s"x: ${x.getClass} is $x")
          x match {
            case obj: java.util.Map[_, _] => mapper.writeValueAsString(obj)
            case obj: Map[_, _] => mapper.writeValueAsString(obj)
            case obj: List[_] => mapper.writeValueAsString(obj)
            case obj: Vector[_] => mapper.writeValueAsString(obj)
            case obj: java.util.ArrayList[_] => mapper.writeValueAsString(obj)
            case _ => x.toString
          }
        }).mkString
      }
    }
  }

  test("bstl expressions") {
    assert(stringExpr("'hello world!'") === "hello world!")
    assert(stringExpr("(2 + 2)") === "4")
    assert(stringExpr("(2 * 3)") === "6")
    assert(stringExpr("((2 * 3) - 3)") === "3")
    assert(stringExpr("join('-', (2 + 2), (2 * 3), (2 + 2))") === "4-6-4")
  }

  test("JsonPath expressions") {
    assert(stringExpr("$.myObj.myFoo") === "foo")
    assert(stringExpr("$.myObj.myTrue") === "true")
    assert(stringExpr("$.myObj.myFalse") === "false")
    assert(stringExpr("$.myObj.myInt") === "42")
    assert(stringExpr("$.myObj.myFloat") === "123.44")
    assert(stringExpr("$.myObj.myArray[1]") === "y")
    assert(stringExpr("$.myObj.myArray[*]") === "xyz")
    assert(stringExpr("$.myObj.myArray") === """["x","y","z"]""")
//    assert(stringExpr("$.myObj.objArray[?(@.k2 == 'o3')].k1") === "true")
    assert(stringExpr("$.myObj.objArray[?(@.k1 == true)].k2") === "o1o3o5")
    assert(stringExpr("$.myObj.objArray[4].k2") === "o5")
  }

  test("javascript embed") {
    assert(stringExpr("javascript(42)") === "42")
    assert(stringExpr("javascript('(2 * 42) - 1')") === "83")
    assert(stringExpr("javascript('function testAdd(l, r) { return parseInt( l + r ) }; \"testAdd\"')") === "testAdd")
    assert(stringExpr("javascript('testAdd(7, 9)')").startsWith("16"))
    assert(stringExpr("join('', 'testAdd(', $.myObj.myInt, ', ', $.myObj.myInt, ')'))").startsWith("testAdd(42"))
    assert(stringExpr("javascript(join('', 'testAdd(', $.myObj.myInt, ', ', $.myObj.myInt, ')'))").startsWith("84"))

  }


}

