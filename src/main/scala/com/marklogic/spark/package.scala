package com.marklogic

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.language.implicitConversions

/**
 * Created by hpuranik on 8/17/2015.
 */
package object spark {
  implicit def addMarkLogicSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)

  implicit def addMarkLogicDataFrameFunctions(df: DataFrame): DataFrameFunctions =
    new DataFrameFunctions(df)

  implicit def addMarkLogicSparkRDDFunctions(rdd: RDD[String]): RDDFunctions[String] =
    new RDDFunctions(rdd)

}
