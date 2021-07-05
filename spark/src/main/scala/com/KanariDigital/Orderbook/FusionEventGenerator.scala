/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook
import com.KanariDigital.app.AppContext
import com.KanariDigital.dsql.AvroTable

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext


import com.KanariDigital.app.AuditLog.CompositeAuditLogger
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.types._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.execution.datasources.hbase._

import java.util.Calendar
import java.text.SimpleDateFormat

import scala.util.parsing.json.JSONObject

import java.math.{RoundingMode, MathContext}
// import java.util.Locale

// import java.lang.management._
// import java.io._

/**
  *
  */
class FusionEventGenerator (
  val spark: org.apache.spark.sql.SparkSession,
  val appContext: com.KanariDigital.app.AppContext) {

  val sqlContext = spark.sqlContext
  val sc = spark.sparkContext
  sc.setCheckpointDir("/user/cloudbreak/ob_hdfs/temp")

  val log: Log = LogFactory.getLog(this.getClass.getName)

  sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

  // FIXME: move to config
  // val avroPath = obContext.obAppConfig.fusionAvroPath.getOrElse("hdfs:///EA/supplychain/raw/EMEA/HEP")
  // THIS VERSION READS AVRO FROM ADLG2 on a cluster that already has keys setup/etc. itgimport is the container name

  val avroPath = "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/raw/EMEA/HEP"

  val appName = "ob_fusion"
  val rddProcessor = new com.KanariDigital.app.RddProcessor(appContext, appName)

  /** return the top level hli for each line item in set of sapons
    */
  def topLineItems( saponsDf: DataFrame, vbapDf: DataFrame) : DataFrame = {
    import sqlContext.implicits._

    // spark user defined function that flattens a list of single item maps into a single map
    val joinMap = udf { values: Seq[Map[String, String]] => values.flatten.toMap }

    // udf that returns a map of (sapitemnum -> topli ) from a map of ( sapitemnum -> hli) 
    val tops = udf {
      hliMap: Map[String, String] => {
        def getTopli(itemnum: String, hlis: Map[String, String]) : String = {
          val hli = hlis.getOrElse(itemnum, "n/a")
          if (hli == "n/a") hli else if (hli == "000000") itemnum else getTopli(hli, hlis)
        }
        for ((x, y) <- hliMap) yield (x-> getTopli(x, hliMap))
      }
    }

    saponsDf.select("client", "sapon").distinct
      .join(vbapDf, Seq("client", "sapon"), "inner")
      .select("client", "sapon", "sapitemnum", "hli")
      .groupBy("client", "sapon").agg(collect_list(map($"sapitemnum",$"hli")) as "map")
      .withColumn("map", joinMap(col("map")))
      .withColumn("map2", tops($"map"))
      .select($"client", $"sapon", explode($"map2"))
      .withColumnRenamed("key", "sapitemnum").withColumnRenamed("value", "topli")
      .join(vbapDf, Seq("client", "sapon", "sapitemnum"), "inner")
  }


  //
  val vbakSrcCols: List[String] = List("mandt", "vbeln", "auart", "erdat", "erzet", "vdatu", "vkorg", "vkgrp", "vtweg", "zzhprecdt", "zz_dccode", "faksk", "lifsk", "autlf", "zzintgr_time", "netwr", "submi", "waerk", "abrvw", "bstnk", "bstdk", "vkbur", "augru", "zzhpquote", "audat", "ihrez", "zz_cdcode", "z1cleandate", "z1cleantime")
  val vbakSemCols: List[String] = List("client", "sapon", "ordertype", "ordercreateddt", "ordercreatetime", "rdd", "salesorganization", "salesgroup", "distributionchannel", "receiveddt", "dccode", "billingblockreasoncode", "deliveryblock", "compldelflag", "intgrtime", "netamountorder", "rtm", "currencyorder", "usage", "customerponumber", "customerpocreateddt", "salesoffice", "orc", "hpquote", "documentdt", "legacyordernum", "cdcode", "headercleaneddt", "headercleanedtime")
  val vbakPK: List[String] = List("client", "sapon")

  val vbak = AvroTable(sqlContext, "vbak", avroPath + "/VBAK", vbakSrcCols, vbakSemCols,
    Some("zlasttimestamp"), Some("vbak_timestamp"), Some(vbakPK), Some("ordertype in ('ZOR', 'ZI1O', 'ZI2O', 'ZNC', 'ZPB')"))

  //
  val vbapSrcCols = List("mandt", "vbeln", "posnr", "abgru", "erdat", "erzet", "kwmeng", "pstyv", "vstel", "werks", "uepos", "grkor", "route", "matnr", "spart", "lprio", "netpr", "netwr", "waerk", "posex", "zzhrtitsi", "z1cleandate", "z1cleantime", "zzchpsup", "zz_ead", "zz_lad")
  val vbapSemCols = List("client", "sapon", "sapitemnum", "rejectionreason", "itemcreateddt", "itemcreatetime", "orderedqty", "itemcategory", "shippingpoint", "plant", "hli", "deliverygroupsap", "route", "materialnum", "productline", "deliverypriority", "itemnetprice", "netamountitem", "currencyitem", "customerpoitemnumber", "legacyitemnum", "itemcleaneddt", "itemcleanedtime", "supplier", "ead", "lad")
  val vbapPK = List("client", "sapon", "sapitemnum")

  val vbapFilter = "plant in ('8O00', 'G100', 'G110', 'G111', 'G112', '01KA', 'B606', '0140', '6303', '0197')"

  val vbap = AvroTable(sqlContext, "vbap", avroPath + "/VBAP", vbapSrcCols, vbapSemCols,
    Some("zlasttimestamp"), Some("vbap_timestamp"), Some(vbapPK), Some(vbapFilter))

  //
  val jcdsSrcCols = List("mandt", "objnr", "stat", "chgnr", "udate", "utime", "inact", "usnam", "tcode")
  val jcdsSemCols = List("client", "objectnumber", "objectstatus", "objectchangenum", "objectdt", "objecttime", "objectinactive", "objectusername", "objecttransactioncode")
  val jcdsPK = List("client", "objectnumber", "objectstatus", "objectchangenum")

  val jcdsFilter = "objectstatus like 'E0%' AND objectnumber like 'VB%'"

  val jcds = AvroTable(sqlContext, "jcds", avroPath + "/JCDS", jcdsSrcCols, jcdsSemCols,
    Some("zlasttimestamp"), Some("jcds_timestamp"), Some(jcdsPK), Some(jcdsFilter))

  //
  val tj30tSrcCols = List("mandt", "stsma", "estat", "txt04", "txt30")
  val tj30tSemCols = List("client", "stsma", "objectstatus", "objectstatuscode", "objectstatusdescription")
  val tj30tPK = List("client", "stsma", "objectstatus",  "objectstatuscode", "objectstatusdescription")
  val tj30t = AvroTable(sqlContext, "tj30t", avroPath + "/TJ30T", tj30tSrcCols, tj30tSemCols,
    Some("zlasttimestamp"), Some("tj30t_timestamp"), Some(tj30tPK), None)

  //
  val jstoSrcCols = List("mandt", "objnr", "stsma")
  val jstoSemCols = List("client", "objectnumber", "stsma")

  val jsto = AvroTable(sqlContext, "jsto", avroPath + "/JSTO", jstoSrcCols, jstoSemCols,
    Some("zlasttimestamp"), Some("jsto_timestamp"), Some(jstoSemCols), Some("objectnumber like 'VB%'"))

  //
  val lipsSrcCols = List("mandt", "vbeln", "posnr", "vgpos", "vgbel", "vbelv", "posnv", "werks", "lfimg")
  val lipsSemCols = List("client", "delivery", "deliveryline", "referenceitemnum", "referencenum", "sapon", "sapitemnum", "deliveryplant", "deliveryqty")
  val lipsPK = List("client", "delivery", "deliveryline")

  val lips = AvroTable(sqlContext, "lips", avroPath + "/LIPS", lipsSrcCols, lipsSemCols,
    Some("zlasttimestamp"), Some("lips_timestamp"), Some(lipsPK), None)

  //
  val likpSrcCols = List("mandt", "vbeln", "lfart", "wadat_ist", "lfdat", "bldat", "podat", "potim", "vstel", "bolnr", "traid", "lifex", "lifnr", "route", "erdat", "erzet")
  val likpSemCols = List("client", "delivery", "deliverytype" , "deliverydt" , "delivereddt" , "documentdt" , "poddt" , "podtime" , "deliveryshippoint" , "billoflading" , "trackingid" , "externaldelivery" , "lspid" , "deliveryroute", "deliverycreateddt", "deliverycreatedtime")
  val likpPK = List("client", "delivery")

  val likp = AvroTable(sqlContext, "likp", avroPath + "/LIKP", likpSrcCols, likpSemCols,
    Some("zlasttimestamp"), Some("likp_timestamp"), Some(likpPK), None)

  //
  // val lfa1SrcCols = List("mandt", "lifnr", "name1")
  // val lfa1SemCols = List("client", "lspid", "lsp")
  // val lfa1PK = List("client", "lspid")
  // val lfa1 = AvroTable(sqlContext, "lfa1", avroPath + "/LFA1", lfa1SrcCols, lfa1SemCols,
  //   Some("zlasttimestamp"), Some("lfa1_timestamp"), Some(lfa1PK), None)


  //
  val vbepSrcCols = List("mandt", "vbeln", "posnr", "etenr", "edatu", "wadat", "lddat", "tddat", "mbdat", "bmeng", "wmeng", "lifsp")
  val vbepSemCols = List("client" , "sapon" , "sapitemnum" , "schedulelinenum" , "planneddeliverydate" , "plannedfactoryshipdate" , "plannedloadingdate" , "plannedtransportationdate" , "plannedmaterialavaildt" , "schedulelineconfirmedqty" , "schedulelineorderedqty" , "schedulelineblocked")
  val vbepPK = List("client" , "sapon" , "sapitemnum" , "schedulelinenum")

  val vbep = AvroTable(sqlContext, "vbep", avroPath + "/VBEP", vbepSrcCols, vbepSemCols,
    Some("zlasttimestamp"), Some("vbep_timestamp"), Some(vbepPK), None)


  //
  val ekkoSrcCols = List("mandt", "ebeln", "aedat", "bsart", "lifnr")
  val ekkoSemCols = List("client" , "supplierponum" , "supplierpocreateddt" , "supplierpotype", "vendornum")
  val ekkoPK = List("client" , "supplierponum")

  val ekko = AvroTable(sqlContext, "ekko", avroPath + "/EKKO", ekkoSrcCols, ekkoSemCols,
    Some("zlasttimestamp"), Some("ekko_timestamp"), Some(ekkoPK), None)

  //
  val ekpoSrcCols = List("mandt", "ebeln", "ebelp", "werks", "loekz")
  val ekpoSemCols = List("client" , "supplierponum" , "supplierpoitemnum" , "factoryplant", "deletionindicator")
  val ekpoPK = List("client" , "supplierponum" , "supplierpoitemnum")

  val ekpo = AvroTable(sqlContext, "ekpo", avroPath + "/EKPO", ekpoSrcCols, ekpoSemCols,
    Some("zlasttimestamp"), Some("ekpo_timestamp"), Some(ekpoPK), None)


  //
  val ekknSrcCols = List("mandt", "ebeln", "ebelp", "zekkn", "vbeln", "vbelp")
  val ekknSemCols = List("client" , "supplierponum" , "supplierpoitemnum", "zekkn", "sapon", "sapitemnum")
  val ekknPK = List("client" , "supplierponum" , "supplierpoitemnum", "zekkn")
  val ekkn = AvroTable(sqlContext, "ekkn", avroPath + "/EKKN", ekknSrcCols, ekknSemCols,
    Some("zlasttimestamp"), Some("ekkn_timestamp"), Some(ekknPK), None)


  //
  val adrcSrcCols = List("client", "addrnumber", "date_from", "nation", "street", "region", "post_code1", "name1", "city1")
  val adrcSemCols = List("client", "adrnr", "date_from", "nation", "street", "region", "postalcode", "customer", "city")
  val adrcPK = List("client", "adrnr", "date_from", "nation")

  val adrc = AvroTable(sqlContext, "adrc", avroPath + "/ADRC", adrcSrcCols, adrcSemCols,
    Some("zlasttimestamp"), Some("adrc_timestamp"), Some(adrcPK), None)

  //
  val vbpaSrcCols = List("mandt", "vbeln", "posnr", "parvw", "adrnr", "land1", "kunnr")
  val vbpaSemCols = List("client", "sapon", "sapitemnum", "parvw", "adrnr", "country", "partyid")
  val vbpaPK = List("client", "sapon", "sapitemnum", "parvw")
  val vbpa = AvroTable(sqlContext, "vbpa", avroPath + "/VBPA", vbpaSrcCols, vbpaSemCols, Some("zlasttimestamp"), Some("vbpa_timestamp"), Some(vbpaPK), Some("(client = '010' and sapon like '7%') or (client = '010' and sapon like '4%')"))


  /**
    *
    */
  class EventTable(val name: String)  {
    var current : Option[DataFrame] = None


    // def setDFName(df: DataFrame, name: String): DataFrame = {
    //   df.createOrReplaceTempView(name)
    //   //df.sparkSession.catalog.cacheTable(name)
    //   df
    // }

    /**
      *
      */
    def addRows(newRows: DataFrame,  writer: Option[(DataFrame, String) => Long] = None) : Long = {
      // val df =  setDFName(newRows, "addRows").persist// newRows.persist
      val df = newRows.checkpoint //persist
      df.persist()

      log.warn(s"adding ${df.count} rows to ${this.name}")
      val written = writer match {
        case Some(wfunc) => {
          // newRows.persist
          wfunc(newRows, this.name)
        }
        case None => 0
      }

      // val allRows = current match {
      //   case Some(oldRows) => {
      //     val newCurrent = oldRows.union(newRows) // .dropDuplicates
      //     newCurrent.persist
      //     //          newCurrent.count
      //     // oldRows.unpersist
      //     // newRows.unpersist
      //     newCurrent
      //   }
      //   case None => newRows
      // }

      // log.warn(s"now have ${allRows.count} rows in ${name}")
      // current = Some(allRows)

      log.warn(s"finished adding ${df.count} rows to ${this.name}")
      df.show(10)

      df.unpersist(true) // blocking
      written
    }
  }

  /**
    *
    */
  object OrderDetails extends EventTable("order_details") {

    /**
      *
      */
    def joinDF(vbak: DataFrame, vbap: DataFrame) : DataFrame = {
      val orders = vbak.join(vbap, Seq("client", "sapon"), "inner")
        .withColumn("serial", greatest(col("vbak_timestamp"), col("vbap_timestamp")))
      orders
    }
  }


  /**
    *
    */
  object OrderStatus extends EventTable("order_status") {

    /**
      *
      */
    def joinDF(ijcds : DataFrame, ijsto: DataFrame, itj30t: DataFrame) : DataFrame = {

      val bigjoin = ijcds.join(ijsto, Seq("client", "objectnumber"))
      val status = bigjoin.join(itj30t, Seq("client", "stsma", "objectstatus"))
        .withColumn("sapon", substring(col("objectnumber"), 3, 10))
        .withColumn("sapitemnum", substring(col("objectnumber"), 13, 6))
        .withColumn("statustype", when(col("sapitemnum") === "000000", "header").otherwise("item"))
        .withColumn("serial", greatest(col("jcds_timestamp"), col("jsto_timestamp")))

      status

    }
  }

  /**
    *
    */
  object OrderPartner extends EventTable("partner") {

    /**
      *
      */
    def joinDF(vbpa: DataFrame, adrc: DataFrame) : DataFrame = {

      val joinCols = List("client", "sapon", "country", "partyid", "street", "region", "postalcode", "customer", "city", "vbpa_timestamp")

      val aAGCols = List("client", "sapon", "soldtocountry", "soldtopartyid", "soldtostreet", "soldtoregion", "soldtopostalcode", "soldtocustomer", "soldtocity",  "AG_timestamp")
      val aWECols = List("client", "sapon", "shiptocountry", "shiptopartypo", "shiptostreet", "shiptoregion", "shiptopostalcode", "shiptocustomer", "shiptocity", "WE_timestamp")
      val aENCols = List("client", "sapon", "endcustomercountry", "endcustomerpartyid", "endcustomerstreet", "endcustomerregion", "endcustomerpostalcode", "endcustomer", "endcustomercity", "RE_timestamp")
      val aRECols = List("client", "sapon", "resellercountry", "resellerpartyid", "resellerstreet", "resellerregion", "resellerpostalcode", "reseller", "resellercity", "EN_timestamp")

      val vbpaAG = vbpa.filter("parvw = 'AG'")
      val jAG  = vbpaAG.join(adrc, Seq("client", "adrnr"), "inner").select(joinCols.head, joinCols.tail:_*).toDF(aAGCols:_*)

      val vbpaWE = vbpa.filter("parvw = 'WE'")
      val jWE  = vbpaWE.join(adrc, Seq("client", "adrnr"), "inner").select(joinCols.head, joinCols.tail:_*).toDF(aWECols:_*)

      val vbpaEN = vbpa.filter("parvw = 'EN'")
      val jEN  = vbpaEN.join(adrc, Seq("client", "adrnr"), "inner").select(joinCols.head, joinCols.tail:_*).toDF(aENCols:_*)

      val vbpaRE = vbpa.filter("parvw = 'RE'")
      val jRE  = vbpaRE.join(adrc, Seq("client", "adrnr"), "inner").select(joinCols.head, joinCols.tail:_*).toDF(aRECols:_*)

      val partners = jAG.join(jWE, Seq("client", "sapon"), "outer")
        .join(jEN, Seq("client", "sapon"), "outer")
        .join(jRE, Seq("client", "sapon"), "outer")
        .withColumn("serial", greatest(col("AG_timestamp"), col("WE_timestamp"), col("EN_timestamp"), col("RE_timestamp")))

      partners

    }
  }

  /**
    *
    */
  object OrderSchedule extends EventTable("schedule") {

    /**
      *
      */
    def joinDF(vbep: DataFrame) : DataFrame = {
      vbep.withColumn("serial", col("vbep_timestamp"))
    }
  }

  /**
    *
    */
  object DeliveryDetails extends EventTable("delivery_details") {

    /**
      *
      */
    def joinDF(likp: DataFrame, lips:DataFrame) : DataFrame = {
      // val delivery_details = likp.join(lips, Seq("client", "delivery"))
      //   .withColumn("serial", greatest(col("likp_timestamp"), col("lips_timestamp")))

      val delivery_details = likp.filter("deliverydt is not null").
        join(lips.filter("sapon is not null"), Seq("client", "delivery"), "inner").
        withColumn("serial", greatest(col("likp_timestamp"), col("lips_timestamp")))

      delivery_details
    }

  }

  /**
    *
    */
  object OrderPurchaseDetails extends EventTable("purchase") {

    /**
      *
      */
    def joinDF(ekko: DataFrame, ekpo: DataFrame, ekkn: DataFrame) : DataFrame = {
      val j1 = ekko.join(ekpo, Seq("client", "supplierponum"))
      val j2 = j1.join(ekkn, Seq("client", "supplierponum", "supplierpoitemnum"))
        .withColumn("serial", greatest(col("ekkn_timestamp"), col("ekko_timestamp"), col("ekpo_timestamp")))
      j2
    }

  }

  /**
    *
    */
  object AllOrders extends EventTable("orders") {
    //FIXME

  }

  var interrupt = false

  /** persist dataframe to HBase via com.KanariDigital.Orderbook.RddProcessor
    *
    */
  def writeHBase(df: DataFrame, tableId: String) : Long = {
    try {

      val rows = df.rdd
      // an RDD of JSON representation of rows
      val jsonRDD = rows.map( row => {
        val fieldNames = row.schema.fieldNames
        val valMap = row.getValuesMap(fieldNames)
        val vm2 = valMap.filter( (y) =>  !Option(y._2).getOrElse("").isEmpty  ) // remove empty fields
        val jsonObj = JSONObject(vm2)
        val myMap: Map[String, JSONObject] = Map("data" -> jsonObj)
        val myJO = JSONObject(myMap)
        val result = myJO.toString()
        result
        // JSONObject(Map( "topicObject" -> JSONObject(row.getValuesMap(row.schema.fieldNames)) )).toString()
      })

      val rddTopicAndJson = jsonRDD.map(x => (tableId, x)).persist

      val start = System.currentTimeMillis()

      rddTopicAndJson.take(10).foreach(println)

      val rowsC = rddTopicAndJson.count()

      val unitOfWork =  com.KanariDigital.app.AuditLog.UnitOfWork.createUnitOfWork()
      val auditLog = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)
      val idocs = 0
      val exceptionMaybe = rddProcessor.processRDD(unitOfWork, rddTopicAndJson, auditLog)
      if (exceptionMaybe.isDefined) {
        throw new Exception(exceptionMaybe.get)
      }
      rddTopicAndJson.unpersist()

      val duration = System.currentTimeMillis() - start
      idocs

    } catch {
      case ex : Exception => {
        log.error(s"HDFS Folder processor, Exception: ${ex.toString()}")
        return -1
      }
    }
  }

  /**
    *
    */
  def loadHBaseTable(template: String) : DataFrame = {

    val catalog = appContext.xform.cfg.hbaseCatalog(template)
    log.warn(s"catalog for $template = $catalog")
    val quorum = appContext.appConfig.hbaseConfigMap.getOrElse("quorum", "")
    log.warn(s"setting ${HConstants.ZOOKEEPER_QUORUM} to $quorum")
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog,
        HConstants.ZOOKEEPER_QUORUM -> quorum))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    //    sqlContext.emptyDataFrame
  }


  /** Load a month bounded range of all source tables from AVRO, and build initial set of events
    *
    */
  def loadInitialAvro(from: String, to: String) : Long = {

    var idocs : Long = 0

    //
    val vbakDF = vbak.loadInitial(from, to)
    val vbapDF = vbap.loadInitial(from, to)

    val orderDetailsDF = OrderDetails.joinDF(vbakDF, topLineItems(vbakDF, vbapDF))
    idocs = idocs + OrderDetails.addRows(orderDetailsDF, Some(writeHBase))

    val jcdsDF = jcds.loadInitial(from, to)
    val jstoDF = jsto.loadInitial(from, to)

    val tj30tDF = tj30t.loadInitial("201512", to)  // start from the beginning for this lookup table

    val orderStatusDF = OrderStatus.joinDF(jcdsDF, jstoDF, tj30tDF)
    OrderStatus.addRows(orderStatusDF, Some(writeHBase))

    //
    val vbpaDF = vbpa.loadInitial(from, to)
    val adrcDF = adrc.loadInitial("201803", to)  // lookup table, has older, stable data

    val orderPartnerDF = OrderPartner.joinDF(vbpaDF, adrcDF)
    OrderPartner.addRows(orderPartnerDF, Some(writeHBase))

    //
    val vbepDF = vbep.loadInitial(from, to)

    val orderScheduleDF = OrderSchedule.joinDF(vbepDF)
    OrderSchedule.addRows(orderScheduleDF, Some(writeHBase))

    //
    val likpDF = likp.loadInitial(from, to)
    // val lfa1DF = lfa1.loadInitial(from, to)
    val lipsDF = lips.loadInitial(from, to)

    val deliveryDetailsDF = DeliveryDetails.joinDF(likpDF, lipsDF)
    DeliveryDetails.addRows(deliveryDetailsDF, Some(writeHBase))

    //
    val ekkoDF = ekko.loadInitial(from, to)
    val ekpoDF = ekpo.loadInitial(from, to)
    val ekknDF = ekkn.loadInitial(from, to)

    val orderPurchaseDetailsDF = OrderPurchaseDetails.joinDF(ekkoDF, ekpoDF, ekknDF)
    OrderPurchaseDetails.addRows(orderPurchaseDetailsDF, Some(writeHBase))

    idocs

  }

  /** Load a month bounded range of all source tables from AVRO, and build initial set of events
    *
    */
  def readInitialAvro(from: String, to: String) : Long = {

    var idocs : Long = 0

    //
    val vbakDF = vbak.loadInitial(from, to)
    val vbapDF = vbap.loadInitial(from, to)

    val jcdsDF = jcds.loadInitial(from, to)
    val jstoDF = jsto.loadInitial(from, to)
    val tj30tDF = tj30t.loadInitial("201512", to)  // start from the beginning for this lookup table

    //
    val vbpaDF = vbpa.loadInitial(from, to)
    val adrcDF = adrc.loadInitial("201803", to)

    //
    val vbepDF = vbep.loadInitial(from, to)
    //
    val likpDF = likp.loadInitial(from, to)
    // val lfa1DF = lfa1.loadInitial(from, to, Some(writeHBase))
    val lipsDF = lips.loadInitial(from, to)

    //
    val ekkoDF = ekko.loadInitial(from, to)
    val ekpoDF = ekpo.loadInitial(from, to)
    val ekknDF = ekkn.loadInitial(from, to)

    idocs

  }

  def updateTables(thisTime: String) : Long = {

    var idocs : Long = 0

    var start = System.currentTimeMillis()

    idocs = idocs + ( vbak.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderDetails.joinDF(df, topLineItems(df, vbap.fullDF.get))
        updates.explain()
        OrderDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })

    var duration = System.currentTimeMillis() - start
    start = System.currentTimeMillis()

    idocs = idocs + (vbap.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderDetails.joinDF(vbak.fullDF.get, topLineItems(df, vbap.fullDF.get))
        updates.explain()
        OrderDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()

    idocs = idocs + (jcds.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderStatus.joinDF(df, jsto.fullDF.get, tj30t.fullDF.get)
        updates.explain()
        OrderStatus.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()

    idocs = idocs + (jsto.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderStatus.joinDF(jcds.fullDF.get, df, tj30t.fullDF.get)
        updates.explain()
        OrderStatus.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()

    idocs = idocs + (tj30t.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderStatus.joinDF(jcds.fullDF.get, jsto.fullDF.get, df)
        updates.explain()
        OrderStatus.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()

    idocs = idocs + (vbpa.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderPartner.joinDF(df, adrc.fullDF.get)
        updates.explain()
        OrderPartner.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start
    start = System.currentTimeMillis()

    idocs = idocs + (adrc.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderPartner.joinDF(vbpa.fullDF.get, df)
        updates.explain()
        OrderPartner.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()

    idocs = idocs + (vbep.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderSchedule.joinDF(df)
        updates.explain()
        OrderSchedule.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    idocs = idocs + (likp.getNew(thisTime) match {
      case Some(df) => {
        val updates = DeliveryDetails.joinDF(df, lips.fullDF.get)
        updates.explain()
        DeliveryDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    // start = System.currentTimeMillis()
    // printMemory(s"loading lfa1:  $thisTime,milliseconds,0")
    // idocs = idocs + (lfa1.getNew(thisTime, Some(writeHBase)) match {
    //   case Some(df) => {
    //     val updates = DeliveryDetails.joinDF(likp.fullDF.get, df, lips.fullDF.get)
    //     updates.explain()
    //     DeliveryDetails.addRows(updates, Some(writeHBase))
    //   }
    //   case _ => 0
    // })
    // duration = System.currentTimeMillis() - start
    // printMemory(s"done loading lfa1:  $thisTime,milliseconds,$duration")

    start = System.currentTimeMillis()

    idocs = idocs + (lips.getNew(thisTime) match {
      case Some(df) => {
        val updates = DeliveryDetails.joinDF(likp.fullDF.get, df)
        DeliveryDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    idocs = idocs + (ekko.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderPurchaseDetails.joinDF(df, ekpo.fullDF.get, ekkn.fullDF.get)
        updates.explain()
        OrderPurchaseDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    idocs = idocs + (ekpo.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderPurchaseDetails.joinDF(ekko.fullDF.get, df, ekkn.fullDF.get)
        updates.explain()
        OrderPurchaseDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    idocs = idocs + (ekkn.getNew(thisTime) match {
      case Some(df) => {
        val updates = OrderPurchaseDetails.joinDF(ekko.fullDF.get, ekpo.fullDF.get, df)
        updates.explain()
        OrderPurchaseDetails.addRows(updates, Some(writeHBase))
      }
      case _ => 0
    })
    duration = System.currentTimeMillis() - start
    idocs

  }

  /** update events tables from the last starting date.
    *  Process one day at a time till caught up, then, if @continue,
    *   keep checking for new updates.
    */
  def catchUp(lastDay: String, continue: Boolean = true) : Long = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val fdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val cal = Calendar.getInstance
    cal.setTime(sdf.parse(lastDay))

    var now = Calendar.getInstance
    var idocs : Long = 0
    while (cal.getTime.compareTo(now.getTime) < 0 ) {
      cal.add(Calendar.DATE, 1)
      val thisTime = fdf.format(cal.getTime)

      log.warn(s"catching up to $thisTime\n")

      val start = System.currentTimeMillis()

      idocs = idocs  + updateTables(thisTime)

      val duration = System.currentTimeMillis() - start

    }
    while (continue) {
      // FIXME: get sleep interval from config
      log.warn(s"napping for 1 hour\n")
      Thread.sleep(60 * (60 * 1000))
      now = Calendar.getInstance
      val thisTime = fdf.format(now.getTime)

      log.warn(s"catching up to $thisTime\n")
      val start = System.currentTimeMillis()

      idocs = idocs  + updateTables(thisTime)
    }
    idocs
  }

}

/** produces Orderbook business process events from datalake of Hana (SAP) tables
  * 
  */
object FusionEventGenerator  {

  def apply(ss: org.apache.spark.sql.SparkSession, configFile: String = "fusion-config.json") : FusionEventGenerator = {

    // we're ignoring hdfsLoc. kept only for backwards compatibility
    val appContext = AppContext(configFile, Some("ob_fusion"))
    new FusionEventGenerator(ss, appContext)

  }
}
