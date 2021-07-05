/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import java.time.Instant

import com.KanariDigital.dsql.AvroTable
import com.KanariDigital.app.AppContext
import com.KanariDigital.app.AuditLog.CompositeAuditLogger
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.functions._

import scala.util.parsing.json.JSONObject

// TODO: This should be removed and replaced with something from the config files once uber objects are declarative
trait UberObjectType extends scala.Equals {
  override def equals(that: Any): Boolean = {
    that match {
      case that: UberObjectType => this.toString() == that.toString()
      case that: String => that.asInstanceOf[String].toLowerCase == this.toString().toLowerCase
      case _ => false }
  }
}
case class OrderObject() extends UberObjectType {
  override def toString: String = {"OrderObject"}
}
case class DeliveryObject() extends UberObjectType {
  override def toString: String = {"DeliveryObject"}
}

object UberObject {
  def apply(uberString:String): UberObjectType = {
    if (uberString.equalsIgnoreCase(OrderObject.toString)) {return OrderObject()}
    if (uberString.equalsIgnoreCase(DeliveryObject.toString)) {return DeliveryObject()}
    throw new IllegalArgumentException
  }
}


/** Business Process Rules Runner.
  *  Applies user defined rules on Business Process Objects assembled from Business Events
  *
  */
class BpRulesRunner (
  val spark: org.apache.spark.sql.SparkSession,
  val appContext: AppContext
) extends Serializable {

  val sqlContext = spark.sqlContext
  val sc = spark.sparkContext

  val log: Log = LogFactory.getLog(this.getClass.getName)
  sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

  val appName = "ob_rules"

  val rddProcessor = new com.KanariDigital.app.RddProcessor(appContext, appName)


  // /**
  //   *
  //   */
  // class OutputTable(val name: String)  {
  //   var current : Option[DataFrame] = None

  //   /**
  //     *
  //     */
  //   def addRows(newRows: DataFrame,  writer: Option[(DataFrame, String) => Long] = None) : Long = {
  //     // val df =  setDfName(newRows, "addRows").persist// newRows.persist
  //     val df = newRows.checkpoint //persist
  //     df.persist()

  //     log.warn(s"adding ${df.count} rows to ${this.name}")
  //     val written = writer match {
  //       case Some(wfunc) => {
  //         // newRows.persist
  //         wfunc(newRows, this.name)
  //       }
  //       case None => 0
  //     }

  //     log.warn(s"finished adding ${df.count} rows to ${this.name}")
  //     df.show(10)

  //     df.unpersist(true) // blocking
  //     written
  //   }
  // }


  /** persist dataframe to HBase via com.KanariDigital.Orderbook.RddProcessor using
    *  RddProcessor and MessageMapper
    */
  def writeHBase(df: DataFrame, tableId: String) : Long = {
    try {
      val startWriteHBase = Instant.now
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
        log.error(s"HDfS Folder processor, Exception: ${ex.toString()}")
        return -1
      }
    }
  }

  /** returns DataFrame representing the HBase table with the schema derived
    *  from MessageMapperConfig
    *
    */
  def loadHBaseTable(template: String) : DataFrame = {

    // loads the table schema
    val catalog = appContext.xform.cfg.hbaseCatalog(template)
    // log.warn(s"catalog for $template = $catalog")
    val quorum = appContext.appConfig.hbaseConfigMap.getOrElse("quorum", "")

    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog,
        HConstants.ZOOKEEPER_QUORUM -> quorum))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

  }

  /** returns DataFrame representing the HBase table with the schema derived
    *  from MessageMapperConfig
    *
    */
  def loadVersionedHBaseTable(template: String, versions: Long = 1) : DataFrame = {

    // loads the table schema
    val catalog = appContext.xform.cfg.hbaseCatalog(template)
    // log.warn(s"catalog for $template = $catalog")
    val quorum = appContext.appConfig.hbaseConfigMap.getOrElse("quorum", "")
    //val mtl =  if (versions > 1) "true" else "false"
    val mtl =  if (versions > 1) "false" else "true"
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog,
        HBaseRelation.MAX_VERSIONS -> s"$versions",
        HBaseRelation.MERGE_TO_LATEST -> mtl,
        HConstants.ZOOKEEPER_QUORUM -> quorum))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

  }

  var dataSets: Map[String, DataFrame] = Map()

  val detailsCols = Seq("client", "sapon", "sapitemnum", "ordertype", "salesorganization", "plant", "deliverypriority", "deliverygroupsap", "route", "supplier", "distributionchannel", "rdd", "cdcode", "dccode", /* "atpcheckflag",*/ "compldelflag", "shippingpoint", "rtm", "productline", "materialnum", "itemcategory", "itemcreateddt", "itemcreatetime", "ordercreateddt", "ordercreatetime", "legacyordernum", "legacyitemnum", "receiveddt", "customerponumber", "customerpoitemnumber", "customerpocreateddt", "orderedqty", "hli", "topli", "hliserial")

  val statusCols = Seq("client", "objectstatuscode", "objectchangenum", "statustype", "objectinactive", "objectstatusdescription", "objectdt", "objecttime", "serial", "objectnumber")

  val purchaseCols = Seq("client", "sapon", "sapitemnum", "supplierponum", "supplierpoitemnum", "factoryplant", "supplierpotype", "key", "vendornum", "serial")

  val scheduleCols = Seq("client", "sapon", "sapitemnum", "plannedmaterialavaildt", "plannedfactoryshipdate", "planneddeliverydate", "schedulelineconfirmedqty", "serial")

  val deliveryCols = Seq("client", "sapon", "sapitemnum", "delivery", "deliverytype", "deliveryplant", "deliveryshippoint", "externaldelivery", "billoflading", "deliverydt", "trackingid", "deliveryqty", "serial")


  val rackSrcCols: List[String] = List("product_id", "del_flag")
  val rackSemCols: List[String] = List("product_id", "del_flag")
  val rackPK: List[String] = List("product_id")

  val rackTable = AvroTable(sqlContext, "rack", "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/raw/bmt/rack_sku_avro", rackSrcCols, rackSemCols,
    None, None, Some(rackPK), None)


  val phwebSrcCols: List[String] = List("pin", "product_line_pin", "product_type")
  val phwebSemCols: List[String] = List("product_id", "productline", "producttype")
  val phwebPK: List[String] = List("product_id")

  val phwebTable = AvroTable(sqlContext, "phweb", "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/raw/Copernicus/phweb", phwebSrcCols, phwebSemCols,
    None, None, Some(phwebPK), None)

  val mmSrcCols = List("client", "materialnum", "materialtype", "productline", "materialgroup")



  /**
    *
    */
  def loadEvents(start: String): Unit = {

    val startLoadEvents = Instant.now

    val purchaseDf = loadHBaseTable("purchase").
      filter(s"serial > '$start'")
    dataSets += ("purchase" -> purchaseDf)

    val scheduleDf = loadVersionedHBaseTable("schedule", 100).filter(s"serial > '$start'").
      filter(s"serial > '$start'")
    dataSets += ("schedule" -> scheduleDf)

    val partnerDf = loadHBaseTable("partner").
      filter(s"serial > '$start'")
    dataSets += ("partner" -> partnerDf)

    val deliveryDf = loadHBaseTable("delivery_details").
      filter(s"serial > '$start'")
    dataSets += ("delivery" -> deliveryDf)

    val deliveryTsDf = loadVersionedHBaseTable("delivery_details", 100).
      filter(s"serial > '$start'")
    dataSets += ("deliveryTs" -> deliveryTsDf)

    val statusDf = loadHBaseTable("order_status").
      filter(s"serial > '$start'").
      select(statusCols.head, statusCols.tail:_*).
      withColumn("sapon", substring(col("objectnumber"), 3, 10)).
      withColumn("sapitemnum", substring(col("objectnumber"), 13, 6)).
      persist
    dataSets += ("status" -> statusDf)

    dataSets += ("headers" -> statusDf.filter("statustype = 'header'").drop("sapitemnum"))
    dataSets += ("items" -> statusDf.filter("statustype = 'item'"))

    val lineItems = loadHBaseTable("order_details").
      filter(s"serial > '$start'").
      withColumnRenamed("serial", "hliserial").
      select(detailsCols.head, detailsCols.tail:_*).persist
    dataSets += ("lineItems" -> lineItems)

    dataSets += ("orders" -> lineItems.filter("hli = '000000'"))

    val atpChecks = loadVersionedHBaseTable("order_atps", 100)
    dataSets += ("atps" -> atpChecks.filter("hli = '000000'").drop("hli"))

    val mmPath = "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/MatMast3"
    val dfReader = sqlContext.read.format("com.databricks.spark.avro")
    val matMast = dfReader.load(mmPath)
    dataSets += ("matMast" -> matMast)

    val rackPath =  "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/rackMap3"
    val rackMap = dfReader.load(rackPath).
      select("product_id", "del_flag").
      withColumnRenamed("product_id", "materialnum").
      withColumn("rackmapped", lit("Y") )
    dataSets += ("rackMap" -> rackMap)

    val scPath = "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/raw/Scout/PHYSICAL_SHIPMENT/"
    val scoutPat = new AvroTable(spark.sqlContext, "scout", scPath, List("primary_shipment_id", "delivered_date",  "pod_submission_date"), List("primary_shipment_id", "delivered_date",  "pod_submission_date"), Some("daytimestamp"), Some("daytimestamp"), None, Some("delivered_date is not null"))

    val scout = scoutPat.loadInitial("201904", "202005").persist
    dataSets += ("scout" -> scout)

    val phweb = phwebTable.loadInitial("201708", "202005").distinct.
      withColumn("materialnum", trim(col("product_id"))).distinct.
      select("materialnum", "producttype").persist
    dataSets += ("phweb" -> phweb)


  }

  def mmRefresh() : Unit = {
    val mmPath = "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/materialmaster/"
    val mmSrcCols = List("client", "materialnum", "materialtype", "plant", "productline", "materialgroup", "materialfamily", "zlasttimestamp")
    val mmSemCols = mmSrcCols
    val mmTable = AvroTable(sqlContext, "matmast", mmPath, mmSrcCols, mmSemCols, None, None, None, None)

    val nmmPath = "abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/matMast3"
    val matMastDf = mmTable.loadInitial("201912", "202005")
    matMastDf.write.mode(org.apache.spark.sql.SaveMode.Overwrite).format("com.databricks.spark.avro").save(nmmPath)
    
  }

  /** returns the named Dataframe, or empty, schemaless dataframe if unable to find it
    */
  def getDf(name: String) : DataFrame = dataSets.getOrElse(name, spark.emptyDataFrame)

  /** extract key components from json */
  def keyedJson(valuesDf: DataFrame): DataFrame =  {

    val ndf = valuesDf.select(json_tuple(col("value"), "client", "sapon", "sapitemnum"), col("value"))
    val keyedDf = ndf.toDF("client", "sapon", "sapitemnum", "json")
    keyedDf
  }

  /** extract key components from json for delivery uber objects */
  def keyedJsonDelivery(valuesDf: DataFrame): DataFrame =  {

    val ndf = valuesDf.select(json_tuple(col("value"), "client", "sapon", "delivery"), col("value"))
    val keyedDf = ndf.toDF("client", "sapon", "delivery", "json")
    keyedDf
  }

  case class MaterialStruct(materialgroup: String, materialfamily: String, rackmapped: String)

  val hcUdf = (client: String, itemcategory: String, materialnum: String, materialgroup: String, materialfamily: String, rackmapped: String, plant: String, productline: String, producttype: String, lines: Seq[Row]) => {

  // val hcUdf = (client: String, itemcategory: String, materialnum: String, ms: MaterialStruct, plant: String, productline: String, producttype: String, linesCount: Long, lines: Seq[Row]) => {

    val linesCount = lines.length

    def isS4(iClient: String): Boolean = {
      (iClient == "300" || iClient == "800")
    }

    def isEsw(iClient: String, iCategory: String): Boolean = {
      if (isS4(iClient)) {
        iCategory == "ZWSE"
      } else {
        if (iCategory == "ZESW" || iCategory == "ZESY") true else false
      }
    }

    def isServices(iClient: String, iCategory: String) : Boolean = {
      if (isS4(iClient)) {
        (iCategory.startsWith("ZF") || iCategory == "ZDDL" || iCategory == "ZSLS" || iCategory == "ZKSL")
      } else {
        (iCategory == "ZTDI" || iCategory == "ZSAS")
      }
    }

    def isServer(iClient: String, iMaterialgroup: String, iProductType: String): Boolean = {
      if (isS4(iClient)) {
        iMaterialgroup == "SERVER"
      } else {
        iProductType == "UN"
      }
    }

    def isIntegrated(numLines: Long) : Boolean = {
      numLines > 1
    }

    def isRack(iClient: String, iCategory: String, iMaterialfamily: String, iRackmapped: String) : Boolean = {
      if (isS4(iClient)) {
        Seq("ZPSH").contains(iCategory)
      } else {
        iMaterialfamily.toLowerCase.startsWith("rack") ||
        iMaterialfamily.toLowerCase.startsWith("cabinet") ||
        iRackmapped == "Y"
      }
    }

    def isNonPhysical(iClient: String, iCategory: String) : Boolean = {
      if (isS4(iClient)) {
        Seq("ZDDB","ZIWB","ZKWB","ZWSB","ZIMS","ZKMS","ZSMS").contains(iCategory)
      } else {
        false
      }
    }

    def isFe(iClient: String, iCategory: String, iMaterialnum: String) : Boolean = {
      if (isS4(iClient)) {
        Seq("ZIFY","ZIFX","ZDDX","ZDDY","ZKFX","ZKFY").contains(iCategory)
      } else {
        ( ! Seq("ZTDI","ZSAS","ZESY","ZESW").contains(iCategory) &&
          ( iMaterialnum.startsWith("HA") || iMaterialnum.startsWith("HA") || iMaterialnum.startsWith("HA")))
      }
    }

    def isBundled(iClient: String, iCategory: String) : Boolean = {
      if (isS4(iClient)) {
        iCategory == "ZSBE"
      } else {
        iCategory == "ZRBH"
      }
    }

    def isAruba(iClient: String, iProductline: String, iPlant: String) : Boolean = {
      if (isS4(iClient)) {
        Seq("34","35","3P","6H","I5","I6","I7","L1","L3","N6","NT","VL","VN","WB","FA","NC","3W").contains(iProductline)
      } else {
        (iPlant == "6303" || iPlant == "01KA")
      }
    }

    // client, itemcategory, materialnum, linesCount, materialgroup productline, plant

    def hasFe(iClient: String, iLines: Seq[Row]): Boolean = {
      val matches = iLines.map{
        case Row(sapitemnum: String, topli: String, hliserial: String, materialnum: String, plant: String, productline: String, itemcategory: String, materialtype: String, materialgroup: String, materialfamily: String, rackmapped: String, producttype: String) => {
          Map("client" -> iClient, "itemcategory" -> itemcategory, "materialnum" -> materialnum)
        }
      }.exists( x => isFe(x("client"), x("itemcategory"), x("materialnum")))
      matches
    }

    if (isEsw(client, itemcategory)) {
      "E-SW"
    } else if (isServices(client, itemcategory)) {
      "Services"
    } else if (isNonPhysical(client, itemcategory)) {
      "OtherNonPhysical"
    } else if (isRack(client, itemcategory, materialfamily, rackmapped) && hasFe(client, lines) && isIntegrated(linesCount)) {
      "RackSolution"
    } else if (isS4(client) && isRack(client, itemcategory, materialfamily, rackmapped) && (isIntegrated(linesCount) || isBundled(client, itemcategory))) {
      "ConfiguredRack"
    } else if (! isS4(client) && isRack(client, itemcategory, materialfamily, rackmapped) && isIntegrated(linesCount)) {
      "ConfiguredRack"
    } else if (hasFe(client, lines) && (isIntegrated(linesCount) || (isServer(client, materialgroup, producttype) && isBundled(client, itemcategory)))) {
      "ComplexCto"
    } else if (isIntegrated(linesCount) || (isServer(client, materialgroup, producttype) && isBundled(client, itemcategory))) {
      "Cto"
    } else if (isServer(client, materialgroup, producttype) || productline == "4A" || productline == "4F") {
      "Bto"
    } else if (isAruba(client, productline, plant)) {
      "PpsAruba"
    } else {
      "Pps"
    }
  }

  val complexityUDF = udf(hcUdf)


  /** return a DataFrame with hlicomplexity column
    */
  def hliComplexities(uberObjDf: DataFrame, lineItems: DataFrame, matMast: DataFrame, phweb: DataFrame, rackMap: DataFrame): DataFrame = {

    val liCols = Seq("client", "sapon", "sapitemnum", "hli", "topli", "hliserial", "materialnum", "plant", "productline", "itemcategory")
    val liDf = lineItems.select(liCols.head, liCols.tail:_*)

    val sapons = uberObjDf.select("client", "sapon").distinct
    val linesDf = sapons.join(liDf, Seq("client", "sapon"), "inner")

    val jdf = linesDf.join(matMast, Seq("client", "materialnum", "plant", "productline"), "left").na.fill("_").
      join(phweb, Seq("materialnum"), "left").na.fill("_").
      join(rackMap, Seq("materialnum"), "left").na.fill("_")

    val lineStruct = struct("sapitemnum", "topli", "hliserial", "materialnum", "plant", "productline", "itemcategory", "materialtype", "materialgroup", "materialfamily", "rackmapped", "producttype")
    val jdf1 = jdf.withColumn("lines", lineStruct)

    val jdf2 = jdf1.groupBy("client", "sapon", "topli").agg(collect_list("lines").as("lines"))

    val jdf3 = linesDf.groupBy("client", "sapon", "topli").count

    val jdf6 = jdf.filter("hli = '000000'").join(jdf2, Seq("client", "sapon", "topli"), "inner").join(jdf3, Seq("client", "sapon", "topli"), "inner")

    val withc = jdf6.withColumn("hlicomplexity", complexityUDF(col("client"), col("itemcategory"), col("materialnum"), col("materialgroup"), col("materialfamily"), col("rackmapped"), col("plant"), col("productline"), col("producttype"), col("lines")))

    val hlicx = withc.select("client", "sapon", "sapitemnum", "hlicomplexity")
    val bigUbers = uberObjDf.join(hlicx, Seq("client", "sapon", "sapitemnum"))

    bigUbers

  }

  def getUberObjects(limit: Int = -1) : DataFrame = {
    val startgetUberObjects = Instant.now

    val d1Df = getDf("orders")
    val d2Df = if (limit > 0) d1Df.limit(limit) else d1Df
    val ordersDf = d2Df.persist
    // dataSets += ("orders" -> ordersDf)

    // val s1 = ordersDf.join(getDf("schedule"), Seq("client", "sapon", "sapitemnum"))

    val l2 = ordersDf.join(getDf("headers"), Seq("client", "sapon"), "left").
      union(ordersDf.join(getDf("items"), Seq("client", "sapon", "sapitemnum"), "left"))


    val d2 = l2.withColumn("combined", struct("objectstatuscode", "objectchangenum", "objectstatusdescription", "statustype", "objectinactive", "objectdt", "objecttime", "objectnumber", "serial")).
      groupBy(detailsCols.head, detailsCols.tail:_*).
      agg(collect_list("combined").as("status_list"))

    val l3Cols = detailsCols :+ "status_list"

    val d3 = d2.join(getDf("schedule"), Seq("client", "sapon", "sapitemnum"), "left").
      withColumn("combined", struct("plannedmaterialavaildt", "plannedfactoryshipdate", "planneddeliverydate", "schedulelinenum", "schedulelineconfirmedqty", "serial")).
      groupBy(l3Cols.head, l3Cols.tail:_*).
      agg(collect_list("combined").as("schedule_list"))

    val l4Cols = l3Cols :+ "schedule_list"

    val d4 = d3.join(getDf("purchase"), Seq("client", "sapon", "sapitemnum"), "left").
      withColumn("combined", struct("supplierponum", "supplierpoitemnum", "supplierpocreateddt", "factoryplant", "supplierpotype", "key", "vendornum", "deletionindicator", "serial")).
      groupBy(l4Cols.head, l4Cols.tail:_*).
      agg(collect_list("combined").as("purchase_list"))

    val l5Cols = l4Cols :+ "purchase_list"

    val d5 = d4.join(getDf("atps"), Seq("client", "sapon", "sapitemnum"), "left").
      withColumn("combined", struct("atpcheckflag", "serial")).
      groupBy(l5Cols.head, l5Cols.tail:_*).
      agg(collect_list("combined").as("atpcheckflag_list"))

    val d6 = d5.join(getDf("partner").select("client", "sapon", "soldtopartyid", "shiptocountry"), Seq("client", "sapon"), "inner")

    val d7 = hliComplexities(d6, getDf("lineItems"), getDf("matMast"), getDf("phweb"), getDf("rackMap"))

    d7
  }


  def deliveryUber(): DataFrame = {
    import sqlContext.implicits._

    val scoutDf1 = getDf("scout").persist

    val deliveryDf = getDf("delivery")
    val deliveryCols = Seq("client", "sapon", "sapitemnum", "delivery", "deliveryline", "deliverytype", "deliveryplant", "deliveryshippoint", "externaldelivery", "billoflading", "trackingid", "deliveryqty")
    val dlv3 =  deliveryDf.select(deliveryCols.head, deliveryCols.tail:_*).filter("sapon is not null").persist

    val gcols = Seq("client", "sapon", "delivery", "deliverytype", "deliveryplant", "deliveryshippoint", "externaldelivery", "billoflading", "trackingid")

    val dlv6 = dlv3.withColumn("combined", struct("sapitemnum", "deliveryline", "deliveryqty")).
      groupBy(gcols.head, gcols.tail:_*).agg(collect_list("combined").as("deliveryline_list")).persist

    // S4 client = 300 or 800
    val dlv010 = dlv6.filter("client = '010'").persist
    val dlvS4E = dlv6.filter("client = '300'").filter("externaldelivery is not null").persist
    val dlvS4D = dlv6.filter("client = '300'").filter("externaldelivery is null").persist

    // S4 with externaldelivery, use that for the join
    val dlvS4EScout = dlvS4E.join(scoutDf1, $"externaldelivery" === $"primary_shipment_id", "left")
    val dlvDScout = dlv010.union(dlvS4D).
      join(scoutDf1, $"delivery" === $"primary_shipment_id", "left")

    val dlvScout = dlvS4EScout.union(dlvDScout).drop("primary_shipment_id").drop("daytimestamp")

    val gcols1 = gcols :+ "deliveryline_list"

    val dlv8 = dlvScout.withColumn("combined", struct("delivered_date", "pod_submission_date")).
      groupBy(gcols1.head, gcols1.tail:_*).agg(collect_list("combined").as("scout_list")).persist

    val deliveryTsDf = getDf("deliveryTs").select("client", "sapon", "delivery", "deliverydt", "serial").
      filter("deliverydt is not null").distinct.
      groupBy("client", "sapon", "delivery", "deliverydt").
      agg(min("serial").cast("long").cast("string").alias("serial")).persist

    val dlv9 = dlv8.join(deliveryTsDf, Seq("client", "sapon", "delivery"))

    val gcols2 = gcols1 :+ "scout_list"
    val dlv10 = dlv9.withColumn("combined", struct("deliverydt", "serial"))
    val dlv11 = dlv10.groupBy(gcols2.head, gcols2.tail:_*).agg(collect_list("combined").as("deliverydt_list")).persist
    dlv11
  }

  @throws(classOf[Exception])
  def getUberObjectsByName(name:String, limit:Int = -1): DataFrame = {
    if (OrderObject() == name) {
      getUberObjects(limit)
    } else if (DeliveryObject() == name) {
      deliveryUber()
    } else {
      throw new IllegalArgumentException(s"$name is not a valid UberObjectType")
    }
  }

  var currentRulesDf:DataFrame = null
  def loadRules(): Unit = {
    log.warn("### LOADING RULES FROM TEST HBASE ###")

    val startLoadRules = Instant.now
    val df = loadHBaseTable("rules")
      .filter(col("key").startsWith("masterrule"))

    // Get latest rule = df.head() since we use key_reverse_timestamp and hbase sort the key
    val masterKey = df.head().getAs("key").toString
    val globalContent = df.head().getAs("content").toString
    val ruleIdsStr = df.head().getAs("ruleorder").toString
    val ruleIds = ruleIdsStr.split('|')
    val filterIds = ruleIds.map(masterKey + "_" + _)
    log.info(s"masterkey: $masterKey")
    log.info(s"ruleIds: $ruleIdsStr")
    currentRulesDf = df.filter(col("key").isin(filterIds:_*))
  }

  def getMasterRules(funcName:String = "MasterRule", uberObjectType: UberObjectType = null): Array[String] = {

    val startGetMasterRules = Instant.now
    val foundRuleMap: Map[String, String] =
      if (uberObjectType != null) {
        appContext.appConfig.uberRulesConfigMap.getOrElse(uberObjectType.toString, Map[String, String]())
      } else {
        appContext.appConfig.rulesConfigMap
      }

    if (foundRuleMap.isEmpty) {
      log.error(s"uberObjectType ${uberObjectType.toString()} not found in config")
      throw new IllegalArgumentException
    }

    val hbaseOrNot = foundRuleMap.getOrElse("mode", "")

    val rules = if(hbaseOrNot != "hbase") {
      var paths: Array[String] = foundRuleMap.getOrElse("files", "").split(",")
      log.info(s"Using rules from: ${paths.mkString(",")}")
      //This loop will pass all the way back up to rules via yield
      for(fPath <- paths) yield {
        var path = new Path(fPath)
        var istream = path.getFileSystem(new HadoopConfiguration).open(path)
        scala.io.Source.fromInputStream(istream).mkString
      }
    } else {
      if (currentRulesDf == null) {
        loadRules()
      }
      // Converting the content column into a list of strings which then i'm delimiting with a new line.
      // the resulting array will be pased up to rules
      currentRulesDf.select("content").collect().map(_(0).asInstanceOf[String]).toList.toArray
    }

    val master = s"""
      |import os; os.environ['KANARI_SPARKMODE']='True'; os.environ['CONFIG_PATH']='${appContext.appConfig.configPath}';spark=False;
      |from KanariDigital import KanariDataHandler
      |def $funcName(context, strparam):
      |    returnVal = KanariDataHandler.TestRunner.sparkRun(context, strparam)
      |    return returnVal
      |
    """.stripMargin

    return master +: rules

  }

  /*
   def getRuleProcessor(funcName:String = "MasterRule"):JythonJsonProcessor = {
   if (currentRulesDf == null) {
   log.error("loadRules must be called before getMasterRule")
   throw new RuntimeException("loadRules must be called before getMasterRule")
   }

   }
   */

  /** prepare order uber dataframe for write to hbase and/or eventhub
    * @return a creates a keyed dataframe
    */
  def prepareWriteOrderUber(uberObjDF: DataFrame): DataFrame = {
    val valueDF = uberObjDF.toJSON.toDF
    val keyedDF = keyedJson(valueDF)
    keyedDF
  }

  /** prepare delivery uber dataframe for write to hbase and/or eventhub
    * @return a creates a keyed dataframe
    */
  def prepareWriteDeliveryUber(uberObjDF: DataFrame): DataFrame = {
    val valueDF = uberObjDF.toJSON.toDF
    val keyedDF = keyedJsonDelivery(valueDF)
    keyedDF
  }

  /** prepare order process dataframe for write to hbase and/or eventhub
    * @return a creates a keyed dataframe
    */
  def prepareWriteOrderProcess(valueDF: DataFrame): DataFrame = {
    val keyedDF = keyedJson(valueDF)
    keyedDF
  }

  /** prepare delivery process dataframe for write to hbase and/or eventhub
    * @return a creates a keyed dataframe
    */
  def prepareWriteDeliveryProcess(valueDF: DataFrame): DataFrame = {
    val keyedDF = keyedJsonDelivery(valueDF)
    keyedDF
  }
}

class BpRulesApply(val spark: org.apache.spark.sql.SparkSession) extends Serializable {
  val sqlContext = spark.sqlContext
  val applicationId = spark.sparkContext.applicationId

  /** return the result of transforming the process objects with the given javascript function
    *
    */
  def applyRules(uberObjects: DataFrame, uberType: UberObjectType, fName: String = "", fDef: Array[String] = Array(), lang: ScriptLanguage = JavascriptScriptLanguage()) : DataFrame = {

    import sqlContext.implicits._

    val startApplyRules = Instant.now

    val uoRdd: RDD[String] = uberObjects.toJSON.rdd
    val lookups = Map("uberType" -> uberType.toString())
    val processed: RDD[String] = if (fName != "" && fDef.length > 0) {
      uoRdd.mapPartitions(recIter => {
          val jt = JythonJsonProcessor(fName, fDef, Array(), applicationId)
          val myList = recIter.toList
          myList.map(x => jt.transform(lookups, x)).iterator
        })
      }else {
        uoRdd
      }

    processed.toDF
  }
}

/**
  *
  */
object BpRulesRunner {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  def apply(spark: org.apache.spark.sql.SparkSession, configFile: String = "/kanari/config/ob/fusion-config.json") : BpRulesRunner = {
    val appContext = AppContext(configFile, Some("ob_rules"))
    this.apply(spark, appContext)
  }

  def apply(spark: org.apache.spark.sql.SparkSession, appContext: AppContext) : BpRulesRunner = {
    new BpRulesRunner(spark, appContext)
  }

  def getUberObjectTypes(): Seq[String] = {
    return Seq(OrderObject.toString(), DeliveryObject.toString())
  }
}
