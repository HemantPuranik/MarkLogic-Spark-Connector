package com.marklogic.spark

import com.marklogic.client.query.{StructuredQueryDefinition, StructuredQueryBuilder}
import org.apache.spark.SparkContext
import scala.language.implicitConversions

/**
 * Created by hpuranik on 8/17/2015.
 */
class SparkContextFunctions (@transient val sc: SparkContext) extends Serializable {

  def newMarkLogicDocumentRDD():  MarkLogicDocumentRDD = {
    //construct a query - only collection query here based on collection name argument.
    new MarkLogicDocumentRDD(sc)
  }

}
