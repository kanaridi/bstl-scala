/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import com.KanariDigital.jdbc.{JdbcManager, jdbcColumnValue}
import org.scalatest.{Tag, Ignore, FunSuite}
import scala.util.{Failure, Success, Try}

object JdbcTest extends Tag("com.KanariDigital.Orderbook.tags.jdbcTest")

class JdbcTest extends FunSuite{
  val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val table = "jsontest"
  val jsonCol = "content"
  val dbUrl = "jdbc:sqlserver://127.0.0.1:1433;"
  val props:Map[String, String] = Map(
    "database" -> "kanari",
    "user" -> "ktech",
    "password" -> "funF00d!"
  )

  val userJson = """{
    "id" : 2,
    "firstName": "John",
    "lastName": "Smith",
    "isAlive": true,
    "age": 25,
    "dateOfBirth": "2015-03-25T12:00:00",
    "spouse": null
  }"""

  val userList:List[String] = List(
    """{"id" : 10, "firstName": "A.", "lastName": "AAA",}""",
    """{"id" : 11, "firstName": "B.", "lastName": "BBB",}""",
    """{"id" : 12, "firstName": "C.", "lastName": "CCC",}""",
    """{"id" : 13, "firstName": "D.", "lastName": "DDD",}""",
    """{"id" : 14, "firstName": "E.", "lastName": "EEE",}""",
    """{"id" : 15, "firstName": "F.", "lastName": "FFF",}""",
    """{"id" : 16, "firstName": "G.", "lastName": "GGG",}""",
    """{"id" : 17, "firstName": "H.", "lastName": "HHH",}""",
    """{"id" : 18, "firstName": "I.", "lastName": "III",}""",
    """{"id" : 19, "firstName": "J.", "lastName": "JJJ",}""",
    """{"id" : 20, "firstName": "K.", "lastName": "KKK",}"""
  )

  ignore("Sql jdbc connection", JdbcTest) {
    //jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE
    val mgr = JdbcManager.create(driver, dbUrl, props, 1000)
    var rowCount = mgr.getConnection() match {
      case Success(conn) => {
        val statement = conn.createStatement()
        val rs = statement.executeQuery("SELECT COUNT(1) FROM jsontest")
        // move to the first row and get the first column which is the count
        rs.next()
        val res = rs.getInt(1)
        conn.close()
        res
      }
      case Failure(e) => {
        println(s"getConnection failed with $e")
        throw(e)
        0
      }
    }
    assert(rowCount > 0)
  }

  ignore("JdbcWriter execute write", JdbcTest) {
    val mgr = JdbcManager.create(driver, dbUrl, props, 1000)
    val query = s"INSERT INTO $table ($jsonCol) VALUES ('$userJson')"
    val result = mgr.executeStatement(query)
    println(result)
  }

  ignore("Jdbc batch write", JdbcTest) {
    val mgr = JdbcManager.create(driver, dbUrl, props, 1000)
    userList.foreach(jString=>
      mgr.addToTableQueue(table, jdbcColumnValue.create("VARCHAR", jString).get)
    )
    val count = mgr.flushAll().get
    assert(count == 11)
  }
}
