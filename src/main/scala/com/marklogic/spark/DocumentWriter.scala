package com.marklogic.spark

import java.io.Serializable
import java.util.UUID

import com.marklogic.client.io.{Format, StringHandle}
import com.marklogic.client.{DatabaseClientFactory, DatabaseClient}
import com.marklogic.datamovement._
import org.apache.spark.{Logging, SparkConf, TaskContext}

/**
 * Created by hpuranik on 5/23/2016.
 */
class DocumentWriter[T](@transient conf : SparkConf) extends Serializable with Logging{

  val mlHost = conf.get("MarkLogic_Host", "localhost")
  val mlPort : Int = conf.getInt("MarkLogic_Port", 8000)
  val mlUser = conf.get("MarkLogic_User")
  val mlPwd = conf.get("MarkLogic_Password")


  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {

        val client: DatabaseClient  = DatabaseClientFactory.newClient(
          mlHost,
          mlPort,
          mlUser,
          mlPwd,
          DatabaseClientFactory.Authentication.valueOf("DIGEST"))

        val moveMgr:DataMovementManager = DataMovementManager.newInstance().withClient(client)

        val batcher : WriteHostBatcher  = moveMgr.newWriteHostBatcher().
                                                  withJobName("RDBBatchDataMover").
                                                  onBatchSuccess(batchSuccessListener).
                                                  onBatchFailure(batchFailureListener)

        val ticket : JobTicket = moveMgr.startJob(batcher)

        while(data.hasNext){
          val rddVal = data.next()
          val isPair : Boolean = rddVal.isInstanceOf
          val doc = data.next().toString
          val id = UUID.randomUUID().toString
          batcher.add(id, new StringHandle(doc).withFormat(Format.JSON))
        }
        batcher.flush()
        moveMgr.stopJob(ticket)
  }

  object batchSuccessListener extends BatchListener[WriteEvent]{

    override def processEvent(databaseClient: DatabaseClient, batch: Batch[WriteEvent]) : Unit =
      logInfo(f"Sucessfully wrote " + batch.getItems().length)
  }

  object batchFailureListener extends BatchFailureListener[WriteEvent]{
    override def processEvent(databaseClient: DatabaseClient, batch: Batch[WriteEvent], throwable: Throwable): Unit =
    logError("FAILURE on batch:" + batch.getJobTicket.getJobId + throwable.toString)
  }

}
