/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl.shell
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.{Logger, Level}
import scala.io.Source

// import com.kanaridi.bstl.parse._
import com.kanaridi.xform._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.kanaridi.bstl.JsonPath

import java.io.{InputStream, FileInputStream}
import java.io.File
import scopt.OParser

case class Config(
  verbose: Boolean = false,
  debug: Boolean = false,
  mapper: File = new File("."),
  mode: String = "",
  topic: String = "",
  files: Seq[File] = Seq())


object Shell {
  // LOG_LEVEL_WARN

//  def pJson(stream: InputStream) : Map[String, Any] = {
  def pJson(stream: InputStream) : Any  = {

    val bufferedSource = Source.fromInputStream(stream)

    val content = bufferedSource.mkString
    bufferedSource.close

    val mapper = new ObjectMapper

    val jobj = mapper.readValue(content, classOf[Object])
    jobj
  }

  def main(args: Array[String]) = {

    Logger.getRootLogger().setLevel(Level.WARN);

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("bstl"),
        head("bstl", "0.1.x"),
        opt[String]('t', "topic")
          .action((x, c) => c.copy(topic = x))
          .text("topic is a required property"),
        opt[String]('m', "mode")
          .action((x, c) => c.copy(mode = x))
          .text("mode is a required property"),
        opt[File]("mapper")
          .valueName("<file>")
          .action((x, c) => c.copy(mapper = x))
          .text("mapper is a transformation mappings file"),
         opt[Unit]('v', "verbose")
          .action((_, c) => c.copy(verbose = true))
          .text("verbose is a flag"),
        opt[Unit]('d', "debug")
          .action((_, c) => c.copy(debug = true))
          .text("dense logging"),
        help("help").text("prints this usage text"),
        arg[File]("<file>...")
          .unbounded()
          .optional()
          .action((x, c) => c.copy(files = c.files :+ x))
          .text("optional unbounded args"),
        note("some notes." + sys.props("line.separator"))
      )
    }


    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        // do something
        println("we're good")
        val topic = config.topic
        val mode = config.mode

        BasicConfigurator.configure()
        val configStream = new FileInputStream(config.mapper)
        // var jobj = Vector[Any]()

        if (config.verbose) {
          Logger.getRootLogger().setLevel(Level.INFO);
        }
        if (config.debug) {
          Logger.getRootLogger().setLevel(Level.DEBUG);
        }

        if (mode == "repl") {
          println("repl")
          var ok = true
          val jobj = pJson(configStream)

          while (ok) {
            val ln = readLine()
            ok = ln != null
            if (ok) {
              println(ln)
              val qr = JsonPath.query(ln, jobj)
              val rv = qr match {
                case Left(x) => "none"
                case Right(x) => {
                  val item0 = qr.right.map(_.toVector).right.get
                  if (item0.length > 0 ) item0(0)
                }
              }
              println(s"$rv")
              print("yes?:")
            }
          }
        } else {
          var lines: String = ""
          var ok = true
          while (ok) {
            val ln = readLine()
            ok = ln != null
            if (ok) lines += ln + "\n"
          }

          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)

          val configStream = new FileInputStream(config.mapper)
          val mmcfg = MessageMapperConfig(configStream)
          val xform = MessageMapperFactory(mmcfg)

          val allRows = xform.transform(topic, lines).map(transformResult => {

            val rows = transformResult.hbaseRows
            rows.map( row => {
              val table = row.tableName
              val key = row.rowKey
              val namespace = row.namespace
              val family = row.family
              val dedup = row.deDupKey
              val sortKey = row.sortKey
              println(s"row: $namespace : $table, $family, $key, deDupKey = $dedup, sortKey = $sortKey")
              val jsonResult = mapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(row.cells);
              println(jsonResult)
            })
          })
          println("Bye all!")
        }
      case _ =>
        println("whuups")
        // arguments are bad, error message will have been displayed
    }
  }
}
