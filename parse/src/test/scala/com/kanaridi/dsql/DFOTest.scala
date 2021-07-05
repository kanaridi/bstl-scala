/* Copyright (c) 2020 Kanari Digital, Inc.. */

package com.kanaridi.dsql

// import com.KanariDigital.dsql.DFO._

import scala.reflect.ClassTag
import scala.util.{Success, Try, Failure}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.holdenkarau.spark.testing.{SharedSparkContext, DataFrameSuiteBase}

import org.scalatest._

import org.scalamock.scalatest.MockFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.kanaridi.xform.{HBRow, MessageMapper, MessageMapperConfig, MessageMapperFactory, TransformResult}

class DFOTest extends FunSuite with DataFrameSuiteBase with MockFactory {

  override implicit def reuseContextIfPossible: Boolean = true

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

  def withGreeting()(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello world"))
  }



  /** read mapper config file and initialize the message mapper */
  def getMessageMapper(path : String) : MessageMapper = {
    println(s"Reading test mapper configuration: " + path)
    //val mmcStream = fileSystem.open(new Path(path.get))
    val mmcStream = getClass().getResourceAsStream(path)
    val mmcfg = MessageMapperConfig(mmcStream)


    val xform = MessageMapperFactory(mmcfg)
    return xform
  }


  // println(s"DFOTest jobj as json '${mapper.writeValueAsString(jobj)}'")

  test ("Compare identical and dissimilar DataFrames") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input1 = sc.parallelize(List(1, 2, 3)).toDF
    assertDataFrameEquals(input1, input1) // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
        assertDataFrameEquals(input1, input2) // not equal
    }

  }

  test ("create and read a dataframe Table wrapper") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    import com.kanaridi.dsql.{SimpleTable, JoinOp} 
    val input1 = sc.parallelize(List(1, 2, 3)).toDF
    val table1 = new SimpleTable(input1)
    assertDataFrameEquals(input1, table1.all.get) // equal
  }

  test ("create and execute Table Join operation") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    import com.kanaridi.dsql.{SimpleTable, JoinOp} 
    val input1 = sc.parallelize(Seq(("alice", 1), ("bob", 2), ("carol", 3))).toDF("name", "val1")
    val table1 = new SimpleTable(input1)

    val input2 = sc.parallelize(Seq(("carol", 4), ("alice", 5), ("bob", 6))).toDF("name", "val2")
    val table2 = new SimpleTable(input2)

    val resultDf = new JoinOp(table1, table2, Seq("name"), "left").all.get.orderBy("name")
    resultDf.show(5)

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("val1", IntegerType, false),
      StructField("val2", IntegerType, true)
    )

    val expectedData = Seq(Row("alice", 1, 5), Row("bob", 2, 6), Row("carol", 3, 4))
    val targetDf = sqlCtx.createDataFrame(sc.parallelize(expectedData), StructType(expectedSchema))
    targetDf.show(5)
    assertDataFrameEquals(resultDf, targetDf) // equal

  }

  test ("create, compose and execute Table Filter and Sort operations") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    import com.kanaridi.dsql.{SimpleTable, JoinOp} 
    val input1 = sc.parallelize(Seq(("alice", 3), ("bob", 1), ("carol", 2))).toDF("name", "val1")
    val table1 = new SimpleTable(input1)

    val resultDfOpt = new FilterOp(new SortOp(table1, "val1"), "val1 > 1").all
    val targetDf = sc.parallelize(Seq(("carol", 2), ("alice", 3))).toDF("name", "val1")
    assertDataFrameEquals(resultDfOpt.get, targetDf) // equal

  }


  test ("apply sparksql function in creating a new column via withColumnOp") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    import com.kanaridi.dsql.{SimpleTable, JoinOp} 
    val input1 = sc.parallelize(Seq(("alice", 3), ("bob", 1), ("carol", 2))).toDF("name", "val1")
    val table1 = new SimpleTable(input1)

    val r1 = new FilterOp(new SortOp(table1, "val1"), "val1 > 1")
    val resultDfOpt = new WithColumnOp(r1, "val2", "concat_ws('-', name, val1)").all

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("val1", IntegerType, false),
      StructField("val2", StringType, false)
    )

    val expectedData = Seq(Row("carol", 2, "carol-2"), Row("alice", 3, "alice-3"))
    val targetDf = sqlCtx.createDataFrame(sc.parallelize(expectedData), StructType(expectedSchema))

    assertDataFrameEquals(resultDfOpt.get, targetDf) // equal

  }

  test("load mappings configuration json") {
    val xform = getMessageMapper("/mappings.json")
    assert(xform.isInstanceOf[MessageMapper])
  }

  test("load DataFrame from String") {
    import org.apache.spark.sql.{Dataset, SparkSession}
    val spark = SparkSession.builder().appName("CsvExample").master("local").getOrCreate()

    import spark.implicits._
    val csvData: Dataset[String] = spark.sparkContext.parallelize(
      """
    |id, date, timedump
    |1, "2014/01/01 23:00:01",1499959917383
    |2, "2014/11/31 12:40:32",1198138008843
  """.stripMargin.lines.toList).toDS()

    val frame = spark.read.option("header", true).option("inferSchema",true).csv(csvData)
    frame.show()
    frame.printSchema()
  }

  test("load dataframe from resource json") {
    val sqlCtx = sqlContext
    val uri = getClass.getResource("/wom-uss.json").toString()


    val df = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(uri)

    // df.printSchema
    // df.show(15)

    import sqlCtx.implicits._
    // val df2 = df.withColumn("dc", explode($"data")).select($"dc.*").withColumn("rows", expr("explode(table)")).select($"rows.*") 
    val df2 = df.withColumn("dc", explode($"data")).select(col("dc.*")).withColumn("rows", expr("explode(table)")).select(col("rows.*"))
    // df2.printSchema
    // df2.show(15)
    // println(s"found ${df2.count} rows")
    assert(df2.count == 56);

  }
  def jsonWriter(df: DataFrame, path: String): Long = {

    df.write.format("json").save(path)
    1
  }

  test("load dataframe from mapping") {
    val xform = getMessageMapper("/test-maps.json")
    assert(xform.isInstanceOf[MessageMapper])

    val fo1 = xform.cfg.getFlowOp("popTable", spark)
    val resultDf = fo1.get.all.get
    resultDf.printSchema
    //    resultDf.toJSON.take(1).foreach(println)
    val fo2 = xform.cfg.getFlowOp("explodeDc", spark)
    val resultDf2 = fo2.get.all.get
    resultDf2.printSchema

    assert (resultDf.count == 56)

    // val vert = xform.cfg.getFlowOp("vertFin1", sqlContext)
    // val df = vert.get.all.get
    // df.printSchema
    // df.show(6)

    // val fojw =  xform.cfg.getFlowOp("writeJson", sqlContext)
    // val rv = fojw.get.run(jsonWriter)


    // assert ( rv == 1 )
  }

  test ("DFOTest - 2") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._ 

    val sourceDF = Seq(
      ("miguel"),
      ("luisa")
    ).toDF("name")

    val actualDF = sourceDF.transform(withGreeting())

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("miguel", "hello world"),
      Row("luisa", "hello world")
    )

    val expectedDF = sqlCtx.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertDataFrameEquals(actualDF, expectedDF)

  }
}


