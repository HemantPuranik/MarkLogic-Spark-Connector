package com.marklogic.sparkexamples

import com.marklogic.spark.addMarkLogicDataFrameFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
 * Created by hpuranik on 5/23/2016.
 */
class RDBBatchDataMover (sparkContext : SparkContext) {

  val sqlContext = new SQLContext(sparkContext)

  def etl() {
    val employees: DataFrame = sqlContext.read.format("jdbc").options(
      Map(
        "driver" -> "oracle.jdbc.driver.OracleDriver",
        "url" -> "jdbc:oracle:thin:ADVENTUREWORKS/1234@engrlab-129-226.engrlab.marklogic.com:1521:xe",
        "dbtable" -> "HR$VEMPLOYEE"
      )).load()
    println("Done Loading DataFrame")

    employees.saveDataFrameToMarkLogic()

    println("Done Saving DataFrame")

  }
}


object RDB2MarkLogicDataMover extends App{
  //first you create the spark context within java
  val sparkConf = new SparkConf().setAppName(
    "com.marklogic.sparkexamples.CreditCardRatingClassifier").setMaster("local")

  sparkConf.set("MarkLogic_Host", "engrlab-129-226.engrlab.marklogic.com")
  sparkConf.set("MarkLogic_Port", "8000")
  sparkConf.set("MarkLogic_User", "admin")
  sparkConf.set("MarkLogic_Password", "admin")
  sparkConf.set("MarkLogic_Database", "Documents")


  val sparkContext = new SparkContext(sparkConf)

  val rdb2MlDataMover = new RDBBatchDataMover(sparkContext)

  rdb2MlDataMover.etl()
}

