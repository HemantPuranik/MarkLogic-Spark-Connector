package com.marklogic.spark

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, Partition, SparkContext}
import org.scalatest.FunSuite


/**
 * Created by hpuranik on 8/14/2015.
 */
class DocumentRDDTest extends FunSuite {

  val sparkConf: SparkConf = new SparkConf().setAppName("com.marklogic.spark.DocumentRDDTest").setMaster("local")
  sparkConf.set("MarkLogic_Host", "engrlab-129-226.engrlab.marklogic.com")
  sparkConf.set("MarkLogic_Port", "8000")
  sparkConf.set("MarkLogic_Database", "VendorHub")
  sparkConf.set("MarkLogic_User", "admin")
  sparkConf.set("MarkLogic_Password", "admin")
  sparkConf.set("MarkLogic_Collection", "NorthCarolina")
  val sc: SparkContext = new SparkContext(sparkConf)

  test("testComputePartitions") {
    val rdd = sc.newMarkLogicDocumentRDD()
    val parts: Array[Partition] = rdd.accessParts

    for(part <- parts){
      println(part.toString)
      val documents: Iterator[JsonNode] = rdd.compute(part, null)
      var count: Int = 0
      while(documents.hasNext){
        val doc: JsonNode = documents.next()
        count += 1
      }
      println("Computed Documents:= " + count)

    }

  }

}
