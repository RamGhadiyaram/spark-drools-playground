//package com.examples
//
//import com.droolsplay.util.SparkSessionSingleton
//import com.google.gson.JsonParser
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.internal.Logging
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//
///**
//  * JsonPlay.
//  */
//object JsonPlay extends App with Logging {
//  Logger.getLogger("org").setLevel(Level.WARN)
//  Logger.getLogger("akka").setLevel(Level.WARN)
//  //  val spark: SparkSession = SparkSession.builder.config("spark.master", "local").getOrCreate;
//  val spark: SparkSession = SparkSessionSingleton.getInstance(Option(this.getClass.getName))
//
//  import spark.implicits._
//
//  val sc = spark.sparkContext
//  val schema = StructType(Seq(
//    StructField("k", StringType, true), StructField("v", DoubleType, true)
//  ))
//  //  from_json
//  var df = sc.parallelize(Seq(
//    ("1", """{"k": "foo", "v": 1.0}""", "some_other_field_1"),
//    ("2", """{"k": "bar", "v": 3.0}""", "some_other_field_2")
//  )).toDF("key", "jsonData", "blobData")
//
//  df = df.withColumn("jsonData", from_json(col("jsonData"), schema))
//
//  df.show(false)
//  df.printSchema()
//  df = spark.emptyDataFrame
//  //get_json_object
//  df = Seq(("""{"name": "Sreenath", "age": 34}""", "")).toDF("name", "age")
//  df.show(false)
//  df.selectExpr("get_json_object(name, '$.name') as name ", "get_json_object(name, '$.age') as age").show(false)
//
//  logInfo("to_json - struct")
//  df = Seq(Tuple1(Tuple1(1))).toDF("a")
//  df.select(to_json($"a")).show(false)
//
//  logInfo("to_json - array")
//  df = Seq(Tuple1(Tuple1(1) :: Nil)).toDF("a")
//  var df2 = Seq(Tuple1(Map("a" -> 1) :: Nil)).toDF("a")
//
//  df.select(to_json($"a")).show(false)
//  df2.select(to_json($"a")).show(false)
//
//
//  logInfo("to_json - map")
//
//  var df1 = Seq(Map("a" -> Tuple1(1))).toDF("a")
//  df2 = Seq(Map("a" -> 1)).toDF("a")
//
//
//  df1.select(to_json($"a")).show(false)
//  df2.select(to_json($"a")).show(false)
//
// val str =  """
//    |{
//    |	"EVENT_HDR": {
//    |		"Header_Name": "ETS",
//    |		"Tkt_Doc_Nb": "20181116064407",
//    |		"Tkt_Doc_Iss_LDt": "20181116064407",
//    |		"Event_Type": "ETCREATE",
//    |		"Rel_Doc": "0062830157067"
//    |	},
//    |
//    |	"ET_TKT_DOC": {
//    |		"Tkt_Doc_Nb": "0062830157067",
//    |		"Tkt_Doc_Iss_LDt": "20181102",
//    |		"Tkt_Doc_Sq_Nb": "1",
//    |		"ET_TKT_DOC_SRC": [{
//    |			"Tkt_Doc_Nb": "0062830157067",
//    |			"Tkt_Doc_Iss_LDt": "20181102",
//    |			"Tkt_Doc_Sq_Nb": "1",
//    |			"TkDc_Rcvd_Src_Cd": "ET",
//    |			"TkDc_Src_Rcvd_GTs": "20181116064407"
//    |		}, {
//    |			"Tkt_Doc_Nb": "0062830157067",
//    |			"Tkt_Doc_Iss_LDt": "20181102",
//    |			"Tkt_Doc_Sq_Nb": "1",
//    |			"TkDc_Rcvd_Src_Cd": "ET",
//    |			"TkDc_Src_Rcvd_GTs": "20181116064407"
//    |		}]
//    |	}
//    |}
//  """.stripMargin
//println(str.toString)
////
////  import com.google.gson.JsonElement
////  import com.google.gson.JsonObject
////  import java.util
////
////  val parser = new JsonParser()
////  val element = parser.parse(str)
////  val obj = element.getAsJsonObject
////  //since you know it's a JsonObject
////  val entries = obj.entrySet //will return members of your object
////  import scala.collection.JavaConversions._
////
////  for (entry <- entries) {
////    println(entry.getKey)
////    println(entry.getValue)
////
////  }
//println("****")
//
//  import com.google.gson.Gson
//  import com.google.gson.JsonArray
//  import com.google.gson.JsonObject
//  import java.util
//
//  val jsonParser = new JsonParser
//  val jo = jsonParser.parse(str).asInstanceOf[JsonObject]
//  val jsonArr = jo.getAsJsonObject("ET_TKT_DOC").getAsJsonArray("ET_TKT_DOC_SRC")
//  //jsonArr.
//  val googleJson = new Gson
//  val jsonObjList: util.ArrayList[_] = googleJson.fromJson(jsonArr, classOf[util.ArrayList[_]])//.toArray().toSeq
//  println("List size is : " + jsonObjList.size)
//  println("List Elements are  : " + jsonObjList.toString)
//import spark.implicits._
//  import org.apache.spark.sql.Encoders
// // val rows: Seq[Row] = jsonObjList.map{ x => Row(  x.toString)}
// // jsonObjList.foreach(println)
//  println(jsonObjList)
// // val rdd = spark.sparkContext.makeRDD[RDD](rows)
//
//}
