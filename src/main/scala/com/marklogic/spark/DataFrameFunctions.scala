package com.marklogic.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame}
import scala.language.implicitConversions

/**
 * Created by hpuranik on 5/23/2016.
 */
class DataFrameFunctions (@transient val df: DataFrame) extends Serializable {

  def saveDataFrameToMarkLogic(collectionName: String =""):  Unit = {

    //val jsonRDD : RDD[String] = df.toJSON
    //val writer =  new DocumentWriter[String](jsonRDD.sparkContext.getConf)
    //jsonRDD.sparkContext.runJob(jsonRDD, writer.write _)

    val rowRDD : RDD[Row] = df.rdd
    val writer =  new RowWriter[Row](rowRDD.sparkContext.getConf)
    rowRDD.sparkContext.runJob(rowRDD, writer.write _)

    //val rowPairRDD :RDD[(String, Row)] = rowRDD.keyBy(row => row.getAs[String](0))
    //val jsonPairRDD : RDD[(String, String)] = rowPairRDD.mapValues(row => row2JSON(row))
  }

  /*
  def row2Key(row : Row): String = {
    row.getAs[String](0)
  }


  def row2JSON(row : Row): String = {
    val fieldNames : Array[String] = row.schema.fieldNames

    val fieldIt : Iterator[String] = fieldNames.iterator

    val builder : StringBuilder = new StringBuilder
    builder.append("{ ")
    while(fieldIt.hasNext) {
      val columnName: String = fieldIt.next()
      val columnVal: String = row.getAs[String](columnName)
      builder.append("\n\"").
        append(columnName).
        append("\": \"").
        append(columnVal).
        append("\"")
      if (fieldIt.hasNext) {
        builder.append(",")
      }
    }
    builder.append("\n}")
    builder.toString()
  }
  */

}
