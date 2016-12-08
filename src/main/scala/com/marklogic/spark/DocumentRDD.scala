package com.marklogic.spark

import com.fasterxml.jackson.databind.JsonNode
import com.marklogic.client.DatabaseClient
import com.marklogic.client.DatabaseClientFactory
import com.marklogic.client.document.{DocumentRecord, GenericDocumentManager, DocumentPage, DocumentManager}
import com.marklogic.client.io.JacksonHandle
import com.marklogic.client.io.marker.{AbstractReadHandle, AbstractWriteHandle}
import com.marklogic.client.query.{StructuredQueryDefinition, StructuredQueryBuilder}
import com.marklogic.datamovement._
import scala.collection.JavaConversions._
import scala.collection.{Set, mutable}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Logging, SparkContext, Partition}

class MarkLogicPartition(val id: Int,
                         val uris: Array[String],
                         val host: String,
                         val forest: String,
                         val port: Int,
                         val dbName: String,
                         val userName: String,
                         val pwd: String,
                         val timeStamp: Long
                         ) extends Partition{

  override def index: Int = id
  override def toString = "index: " + index +
                          ", host: " + host +
                          ", forest: " + forest +
                          ", database: " + dbName +
                          ", URI Count: " + uris.length
}

/**
 * Created by hpuranik on 8/14/2015.
 */
class MarkLogicDocumentRDD(@transient sc: SparkContext) extends RDD[JsonNode](sc, Nil) with Logging{

  var  partitionMap: mutable.HashMap[String, mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]]] = null

  val mlHost = sc.getConf.get("MarkLogic_Host", "localhost")
  val mlPort : Int = sc.getConf.getInt("MarkLogic_Port", 8000)
  val mlDatabaseName: String = sc.getConf.get("MarkLogic_Database")
  val mlUser = sc.getConf.get("MarkLogic_User")
  val mlPwd = sc.getConf.get("MarkLogic_Password")
  val mlCollectionName = sc.getConf.get("MarkLogic_Collection")




  def printRDD(): Unit = {

    val query: StructuredQueryDefinition = new StructuredQueryBuilder().collection(mlCollectionName)
    val secCtx: DatabaseClientFactory.SecurityContext = new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd)
    val client: DatabaseClient = DatabaseClientFactory.newClient(mlHost, mlPort, mlDatabaseName, secCtx)

    val docMgr: DocumentManager[_ <: AbstractReadHandle, _ <: AbstractWriteHandle] = client.newXMLDocumentManager
    var start: Long = 1
    var page: DocumentPage = null
    var docIndex: Int = 0
    while (start == 1 || page.hasNextPage) {
      page = docMgr.search(query, start)
      println("Printing URIs from query results " +  page.getPageNumber + "/" + page.getTotalPages)
      for (doc <- page) {
        docIndex += 1
        println(docIndex + " => " + doc.getUri)
      }
      start += page.getPageSize
    }
  }


  def accessParts: Array[Partition] = getPartitions

  override protected def getPartitions: Array[Partition] = {

    partitionMap = new mutable.HashMap[String, mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]]]

    val secCtx: DatabaseClientFactory.SecurityContext = new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd)
    val client: DatabaseClient = DatabaseClientFactory.newClient(mlHost, mlPort, mlDatabaseName, secCtx)
    val moveMgr:DataMovementManager = DataMovementManager.newInstance().withClient(client)
    val query: StructuredQueryDefinition = new StructuredQueryBuilder().collection(mlCollectionName)
    val uriBatcher : QueryHostBatcher = moveMgr.newQueryHostBatcher(query).
                                                  withConsistentSnapshot().
                                                  withJobName("RDD Creation").
                                                  withBatchSize(20000).
                                                  onUrisReady(new batchReady).
                                                  onQueryFailure(new queryFailed)

    val uriBatcherTicket: JobTicket = moveMgr.startJob(uriBatcher)
    uriBatcher.awaitCompletion()
    moveMgr.stopJob(uriBatcherTicket)

    /* organize all the partitions within a host in breadth first manner
       For example 3 hosts 3 forests and 3 partitions each
        H1 F1 P1
        H1 F2 P1
        H1 F3 P1
        H1 F1 P2
        H1 F2 P2
        H1 F3 P2
        H1 F1 P3
        H1 F2 P3
        H1 F3 P3
        H2 F1 P1
        H2 F2 P1
        H2 F3 P1
        H2 F1 P2
        H2 F2 P2
        H2 F3 P2
        H2 F1 P3
        H2 F2 P3
        H2 F3 P3
        H3 F1 P1
        H3 F2 P1
        H3 F3 P1
        H3 F1 P2
        H3 F2 P2
        H3 F3 P2
        H3 F1 P3
        H3 F2 P3
        H3 F3 P3
     */
    val hosts: Set[String] = partitionMap.keySet
    val hostCount: Int = hosts.size
    val hostSplits: Array[ArrayBuffer[MarkLogicPartition]] = new Array[ArrayBuffer[MarkLogicPartition]](hostCount)
    var hostIndex: Int = 0
    for (host <- hosts) {
      val forestSplitLists: mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]] = partitionMap.getOrElse(host, null)
      val hostForests: Set[String] = forestSplitLists.keySet
      //walk through breadth first manner
      hostSplits(hostIndex) = new ArrayBuffer[MarkLogicPartition]
      var more: Boolean = true
      var distro: Int = 0
      while(more) {
        more = false
        for(hostForest <- hostForests){
          val forestPartitions: ArrayBuffer[MarkLogicPartition] = forestSplitLists(hostForest)
          if(distro < forestPartitions.size){
            hostSplits(hostIndex) += forestPartitions.get(distro)
          }
          more = more || ( distro+1 < forestPartitions.size)
        }
        distro +=1
      }
      hostIndex +=1
    }

    /* organize all the partitions across all hosts in breadth first manner for optimal parallel cluster computation
       For example 3 hosts 3 forests and 3 partitions each
        H1 F1 P1
        H2 F1 P1
        H3 F1 P1
        H1 F2 P1
        H2 F2 P1
        H3 F2 P1
        H1 F3 P1
        H2 F3 P1
        H3 F3 P1
        H1 F1 P2
        H2 F1 P2
        H3 F1 P2
        H1 F2 P2
        H2 F2 P2
        H3 F2 P2
        H1 F3 P2
        H2 F3 P2
        H3 F3 P2
        H1 F1 P3
        H2 F1 P3
        H3 F1 P3
        H1 F2 P3
        H2 F2 P3
        H3 F2 P3
        H1 F3 P3
        H2 F3 P3
        H3 F3 P3
     */
    val partitions: ArrayBuffer[MarkLogicPartition] = new ArrayBuffer[MarkLogicPartition]
    var more: Boolean = true
    var distro: Int = 0
    while(more){
      more = false
      for(splitListPerHost <- hostSplits){
        if(distro < splitListPerHost.size){
          partitions.add(splitListPerHost.get(distro))
        }
        more = more || ( distro+1 < splitListPerHost.size)
      }
      distro += 1
    }
    for(part <- partitions){
      println(part.toString)
      val documents: Iterator[JsonNode] = compute(part, null)
      var count: Int = 0
      while(documents.hasNext){
        val doc: JsonNode = documents.next()
        count += 1
      }
      println("Computed Documents:= " + count)

    }
    partitions.toArray




    /*
    val maxSplitSize: Long = 20000
    val buf: StringBuilder = new StringBuilder
    buf ++= "xquery version \"1.0-ml\";\n"
    buf ++= "import module namespace hadoop = "
    buf ++= "\"http://marklogic.com/xdmp/hadoop\" at "
    buf ++= "\"/MarkLogic/hadoop.xqy\";\n"
    buf ++= "hadoop:get-splits("
    buf ++= "'', \'"
    buf ++= "fn:collection("
    if (collectionName.length > 0) {
      buf ++= "\""
      buf ++= collectionName
      buf ++= "\""
    }
    buf ++= ")"
    buf ++= "\', \'"
    buf ++= "()"
    buf ++= "\')"
    val splitQuery: String = buf.toString()
    println(splitQuery)


    val forestSplits: ArrayBuffer[ForestSplit] = new ArrayBuffer[ForestSplit]
    //println("Number of forest splits = " + forestSplits.size)
    val client: DatabaseClient = DatabaseClientFactory.newClient(host, port, user, pwd,
                                                            DatabaseClientFactory.Authentication.valueOf("DIGEST"))
    val result: EvalResultIterator = client.newServerEval.xquery(splitQuery).eval
    var count: Int = 0
    while (result.hasNext) {
      val item: EvalResult = result.next
      val index: Int = count % 3
      if (index == 0) {
        //System.out.println("Forest ID Type = " + item.getType.toString)
        val split: ForestSplit = new ForestSplit()
        split.forestId = BigInt(item.getString)
        forestSplits += split
      }
      else if (index == 1) {
        //System.out.println("Forest Record Count Type = " + item.getType.toString)
        forestSplits.get(forestSplits.size - 1).recordCount = item.getNumber.longValue
      }
      else if (index == 2) {
        //System.out.println("Host Name Type = " + item.getType.toString)
        forestSplits.get(forestSplits.size - 1).hostName = item.getString
      }
      count += 1
    }
    //println("Fetched " + forestSplits.size + " forest splits.")
    var forestIndex: Int = 0
    for (split <- forestSplits) {
      forestIndex += 1
      println(forestIndex+"=>ForestID="+split.forestId+",RecordCount="+split.recordCount+",Host="+split.hostName)
    }
    val hostForestSplits: mutable.Map[String, ArrayBuffer[ArrayBuffer[MarkLogicPartition]]] =
                                                          new mutable.HashMap[String, ArrayBuffer[ArrayBuffer[MarkLogicPartition]]]
    //println("Number of forest splits = " + forestSplits.length)
    for (i <- forestSplits.indices) {
      val fsplit = forestSplits(i)
      if (fsplit.recordCount > 0) {
        var splits: ArrayBuffer[MarkLogicPartition] = null
        val host: String = fsplit.hostName
        var splitLists: ArrayBuffer[ArrayBuffer[MarkLogicPartition]] =
                if(hostForestSplits.contains(host)) hostForestSplits(host) else null
        if (splitLists == null) {
          splitLists = new ArrayBuffer[ArrayBuffer[MarkLogicPartition]]
          hostForestSplits(host) = splitLists
        }
        splits = new ArrayBuffer[MarkLogicPartition]
        splitLists += splits

        if (fsplit.recordCount < maxSplitSize) {
          val split: MarkLogicPartition = new MarkLogicPartition(0, 0, fsplit.recordCount, fsplit.forestId, fsplit.hostName)
          split.isLastPartition = true
          splits += split
          println("Added split " + split.toString)
        }
        else {
          var splitCount: Long = fsplit.recordCount / maxSplitSize
          var remainder: Long = fsplit.recordCount % maxSplitSize
          if (remainder != 0) {
            splitCount += 1
          }
          var splitSize: Long = fsplit.recordCount / splitCount
          remainder = fsplit.recordCount % splitCount
          if (remainder != 0) {
            splitSize += 1
          }
          if ((splitSize & 0x1) != 0) {
            splitSize += 1
          }
          var remainingCount: Long = fsplit.recordCount
          var partitionIndex: Int = 0
          while (remainingCount > 0) {
            val start: Long = fsplit.recordCount - remainingCount
            val length: Long = splitSize
            val split: MarkLogicPartition = new MarkLogicPartition(partitionIndex, start, length,
              fsplit.forestId, fsplit.hostName)
            partitionIndex += 1
            if (remainingCount <= maxSplitSize) {
              split.isLastPartition = true
            }
            splits += split
            remainingCount -= length
            println("Added split " + split)
          }
        }
      }
    }

    // mix the lists of splits into one per host
    val hosts: Set[String] = hostForestSplits.keySet
    val hostCount: Int = hosts.size
    val hostSplits: Array[ArrayBuffer[MarkLogicPartition]] = new Array[ArrayBuffer[MarkLogicPartition]](hostCount)
    var hostIndex: Int = 0
    for (host <- hosts) {
      val forestSplitLists: ArrayBuffer[ArrayBuffer[MarkLogicPartition]] = hostForestSplits(host)
      if(forestSplitLists.size == 1){
        hostSplits(hostIndex) = forestSplitLists.get(0)
        hostIndex +=1
      }else{
        //walk through breadth first manner
        hostSplits(hostIndex) = new ArrayBuffer[MarkLogicPartition]
        var more: Boolean = true
        var distro: Int = 0
        while(more) {
          more = false
          for(splitsPerForest <- forestSplitLists){
            if(distro < splitsPerForest.size){
              hostSplits(hostIndex) += splitsPerForest.get(distro)
            }
            more = more || ( distro+1 < splitsPerForest.size)
          }
          distro +=1
        }
        hostIndex +=1
      }
    }

    // mix hostSplits into one - walk through breadth first manner
    val partitions: ArrayBuffer[MarkLogicPartition] = new ArrayBuffer[MarkLogicPartition]
    var more: Boolean = true
    var distro: Int = 0
    while(more){
      more = false
      for(splitListPerHost <- hostSplits){
        if(distro < splitListPerHost.size){
          partitions.add(splitListPerHost.get(distro))
        }
        more = more || ( distro+1 < splitListPerHost.size)
      }
      distro += 1
    }

    partitions.toArray
   */

  }

  override def compute(split: Partition, context: TaskContext): Iterator[JsonNode] = {

    val part: MarkLogicPartition = split.asInstanceOf[MarkLogicPartition]
    println(part.toString)

/*
    val start: Long = part.start + 1
    val end: Long = if (part.isLastPartition) Long.MaxValue else start + part.length - 1
    val docSelectorBuf: StringBuffer = new StringBuffer
    docSelectorBuf.append("fn:collection(")
    if (collectionName.length > 0) {
      docSelectorBuf.append("\"")
      docSelectorBuf.append(collectionName)
      docSelectorBuf.append("\"")
    }
    docSelectorBuf.append(")")

    val docExpr: String = docSelectorBuf.toString
    val subExpr: String = ""
    val indent: String = "FALSE"
    val buf: StringBuilder = new StringBuilder
    buf ++= "xquery version \"1.0-ml\"; \n"
    buf ++= "declare option xdmp:output \"indent=no\";"
    buf ++= "declare option xdmp:output \"indent-untyped=no\";"
    buf ++= "xdmp:with-namespaces(("
    buf ++= "),fn:unordered(fn:unordered("
    buf ++=  docExpr
    buf ++= ")["
    buf ++= start.toString
    buf ++= " to "
    buf ++= end.toString
    buf ++= "]"
    buf ++= subExpr
    buf ++= "))"
    val queryText: String = buf.toString

    println(queryText)
*/
    // fetch data from server
    val partitionDocuments: ArrayBuffer[JsonNode] = new ArrayBuffer[JsonNode]
    val secCtx: DatabaseClientFactory.SecurityContext =
                  new DatabaseClientFactory.DigestAuthContext(part.userName, part.pwd)
    val client: DatabaseClient = DatabaseClientFactory.newClient(part.host, part.port, part.dbName, secCtx)
    val docMgr: GenericDocumentManager = client.newDocumentManager()
    println("Page URI Count = " + part.uris.length)
    val page: DocumentPage = docMgr.read(part.timeStamp, part.uris:_*)

    //val result: EvalResultIterator = client.newServerEval.xquery(query.toString).eval
    while (page.hasNext) {
      val record: DocumentRecord = page.next()
      val handle:JacksonHandle = record.getContent(new JacksonHandle())
      val doc: JsonNode = handle.get
      partitionDocuments.add(doc)
    }

    partitionDocuments.iterator
  }



  class ForestSplit {
    private[MarkLogicDocumentRDD] var forestId: BigInt = null
    private[MarkLogicDocumentRDD] var hostName: String = null
    private[MarkLogicDocumentRDD] var recordCount: Long = 0L
  }

  class batchReady extends BatchListener[String] {
    override def processEvent(databaseClient: DatabaseClient, batch: Batch[String]): Unit = {
      val idx: Int = batch.getJobBatchNumber.toInt
      val host: String = batch.getForest.getHostName
      val forest: String = batch.getForest.getForestName
      var forestParts: mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]] = partitionMap.getOrDefault(host, null)
      //println(partitionMap.getClass.getCanonicalName)
      if(forestParts == null){
        //host encountered for the first time
        forestParts = new mutable.HashMap[String,ArrayBuffer[MarkLogicPartition]]
        partitionMap.put(host, forestParts)
      }
      var parts: ArrayBuffer[MarkLogicPartition] = forestParts.getOrDefault(forest, null)
      if(parts == null){
        //forest encountered for the first time
        parts = new ArrayBuffer[MarkLogicPartition]
        forestParts.put(forest, parts)
      }
      val timestamp: Long = batch.getServerTimestamp
      parts.add(new MarkLogicPartition(idx,
                                        batch.getItems,
                                        host,
                                        forest,
                                        databaseClient.getPort,
                                        databaseClient.getDatabase,
                                        databaseClient.getUser,
                                        databaseClient.getPassword,
                                        timestamp))
      logInfo(f"Sucessfully Added Partition" + batch.getJobBatchNumber)
    }
  }

  class queryFailed extends FailureListener[QueryHostException] {
    override def processFailure(databaseClient: DatabaseClient, e: QueryHostException): Unit = {
      logInfo(e.printStackTrace().toString)
    }
  }

}

