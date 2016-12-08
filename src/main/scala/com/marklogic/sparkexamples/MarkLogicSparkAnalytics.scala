package com.marklogic.sparkexamples

import com.fasterxml.jackson.databind.JsonNode
import com.marklogic.spark.{MarkLogicDocumentRDD, addMarkLogicSparkContextFunctions}
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by hpuranik on 12/5/2016.
 */
class MarkLogicSparkAnalytics (val sc : SparkContext) {

  def analyzeMarkLogicJSONData() {
    val mlRDD: MarkLogicDocumentRDD = sc.newMarkLogicDocumentRDD()
    println("Done Loading MarkLogic RDD. Docs Loaded = " + mlRDD.count())
  }
}

object MarkLogicDataAnalyzer extends App{
  //first you create the spark context within java
  val sparkConf = new SparkConf().setAppName(
    "com.marklogic.sparkexamples.MarkLogicDataAnalyzer").setMaster("local")

  sparkConf.set("MarkLogic_Host", "engrlab-129-226.engrlab.marklogic.com")
  sparkConf.set("MarkLogic_Port", "8000")
  sparkConf.set("MarkLogic_Database", "VendorHub")
  sparkConf.set("MarkLogic_User", "admin")
  sparkConf.set("MarkLogic_Password", "admin")
  sparkConf.set("MarkLogic_Collection", "NorthCarolina")

  val sparkContext = new SparkContext(sparkConf)
  val mlAnalyzer = new MarkLogicSparkAnalytics(sparkContext)
  mlAnalyzer.analyzeMarkLogicJSONData()
}
