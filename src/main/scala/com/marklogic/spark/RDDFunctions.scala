package com.marklogic.spark

import org.apache.spark.rdd.RDD
import scala.language.implicitConversions

/**
 * Created by hpuranik on 5/23/2016.
 */
class RDDFunctions[String](@transient val rdd: RDD[String]) extends Serializable {

  def saveRDDToMarkLogic(collectionName: String):  Unit = {

    val writer =  new DocumentWriter[String](rdd.sparkContext.getConf)

    rdd.sparkContext.runJob(rdd, writer.write _)
  }

}
