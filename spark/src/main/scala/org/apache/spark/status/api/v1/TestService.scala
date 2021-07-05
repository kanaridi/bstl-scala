/* Copyright (c) 2020 Kanari Digital, Inc. */

package org.apache.spark.status.api.v1

import javax.ws.rs.{GET, POST, Path, PathParam, QueryParam, Produces, Consumes}
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.kanaridi.bstl.JsonPath

import com.kanaridi.rest.Service

/**
  *  hijack the spark web UI (jetty / jersey) by injecting a few API paths of our own under "/api/bst"
  *  this all works, automagically, because we happen to be in package "org.apache.spark.status.api.v1"
  *  Paths defined here will be at http:[host]:[port]/api/bst/[path]
  */

@Path("/bst")
class TestService extends {

  // simple api test
  @GET
  @Path("sum/{x}/{y}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getApplicationList(
    @PathParam("x") x: Int,
    @PathParam("y") y: Int): Int = {
    x+y
  }

  // try out an xform mapfile
  @POST
  @Path("runMap")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def postRunMap(body: String): String = {

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jobj = mapper.readValue(body, classOf[Object])

    val topic = mapper.writeValueAsString(jobj.asInstanceOf[Map[Any, Any]].getOrElse("topic", "none"))
    val mapFile = mapper.writeValueAsString(jobj.asInstanceOf[Map[Any, Any]].getOrElse("mapFile", Map[Any, Any]()))
    val srcData = mapper.writeValueAsString(jobj.asInstanceOf[Map[Any, Any]].getOrElse("data", Map[Any, Any]()))
    Service.runMap(topic, mapFile, srcData)
  }


  // return metadata and a sample from running a flow spec to the specified signal(sinkName)
  @POST
  @Path("flow/run/{sinkName}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def postFlowRun(
    @PathParam("sinkName") sinkName: String,
    @QueryParam("limit") limit: Int,
    @QueryParam("disableEmbargo") disableEmbargo: Boolean,
    body: String): String = {
    Service.runFlowV2(body, sinkName, limit, false, false, disableEmbargo)
  }

  // return metadata and a sample from running a flow spec with side-effects disabled
  @POST
  @Path("flow/test/{sinkName}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def postFlowTest(
    @PathParam("sinkName") sinkName: String,
    @QueryParam("limit") limit: Int,
    @QueryParam("previewMode") previewMode: Boolean,
    @QueryParam("disableEmbargo") disableEmbargo: Boolean,
    body: String): String = {
    Service.runFlowV2(body, sinkName, limit, true, previewMode, disableEmbargo)
  }

}
