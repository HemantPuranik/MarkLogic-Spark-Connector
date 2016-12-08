# MarkLogic Native Spark Connector Prototype
Rough Prototype of MarkLogic Native Spark Connector

## MarkLogic Spark Connector ##

Apache Spark is a powerful open source cluster computing engine built for sophisticated analytics. When you think about Big Data workloads, Apache Spark architecture is built around speed and in-memory analytics whereas MarkLogic is built to handle operational use cases than involve transaction management, search and real time query work loads across heterogeneous data sets at scale. Apache Spark and MarkLogic are different but complementary at the same time. 

As demonstrated in [MarkLogic Spark Examples](https://github.com/HemantPuranik/MarkLogicSparkExamples), you can use [MarkLogic connector for Hadoop](http://developer.marklogic.com/products/hadoop) to develop your Spark application. While this is a fine approach, there is an opportunity to build tighter integration between MarkLogic and Apache Spark that does not rely on Hadoop. 

 
## Requirements for native MarkLogic Spark Connector##

1. MarkLogic as data source for Apache Spark pipeline - Use Case 1. Provide ability to load MarkLogic documents into a Spark RDD. A Spark programmer should be allowed to pass the query when creating MarkLogic RDD. The query should be executed within MarkLogic server and query results should be made available within Spark RDD. RDD represents a distributed data set across the Spark cluster. Once MarkLogic data is available in Spark RDD, programmers can develop Spark data transformation pipeline. This way a Spark programmer can use their Spark skill set along with MarkLogic indexes that are leveraged when query is executed within MarkLogic server.      
2. MarkLogic as data source for Apache Spark pipeline - Use Case 2. Provide ability to load MarkLogic data into Spark DataFrame. A DataFrame represents distributed data set similar to RDD but DataFrame is mainly for structured data. Ideally MarkLogic DataFrame would leverage row index in MarkLogic 9. It would take the query based on optic API and return a set of rows that would be available within DataFrame. Note that Spark DataFrame is a primary mechanism to build a machine learning pipeline within Apache Spark. 
3. MarkLogic as target for Apache Spark pipeline - Use Case 1. Provide ability to save an arbitrary RDD into MarkLogic database. An arbitrary RDD where each object can be accessed as an id or URI and its content as document can be saved into MarkLogic. This capability can be used to migrate document data from another database into MarkLogic. Apache Spark can be used to transform the data during migration.   
4. Provide ability to save an arbitrary DataFrame into MarkLogic. This capability can be used to migrate relational data from another database into MarkLogic. Apache Spark can be used to transform the relational data during Migration. Another use case is to execute a machine learning pipeline either on MarkLogic data set or any other data set and store the results of the pipeline back into MarkLogic.
5. Make sure that MarkLogic Spark Connector API looks Spark friendly i.e. consistent with how  vendor specific capabilities are made available to Spark developer community. 

## Which requirements are addressed via this prototype and How##
1. Prototype very roughly demonstrates how to support loading MarkLogic query results into a Spark RDD. The current implementation of [MarkLogicDocumentRDD](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/DocumentRDD.scala) assumes that query results are all JsonNode. Note that [MarkLogicDocumentRDD](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/DocumentRDD.scala) overrides two functions of the parent class *getPartitions* and *compute* with MarkLogic specific implementation. The function *getPartitions* adapts the query partitioning approach demonstrated by [MarkLogic Query Partitioner](https://github.com/HemantPuranik/MarkLogicDMSDKExperiments). This function is called one time when RDD is created. It executes the query job using ML9 [QueryHostBatcher](https://github.com/marklogic/data-movement/blob/develop/data-movement/src/main/java/com/marklogic/datamovement/QueryHostBatcher.java). It creates the *MarkLogicPartition* objects that extends the Spark *Partition*. The function compute is called for *TaskContext*. This function will be called in a distributed manner across multiple JVMs that will compute the partitions of the RDD that they are managing locally. The implementation of the compute function is similar to that of [ExportListener](https://github.com/marklogic/data-movement/blob/develop/data-movement/src/main/java/com/marklogic/datamovement/ExportListener.java). 
2. Prototype demonstrates how to save an arbitrary DataFrame into MarkLogic. The functionality is implemented in the class [RowWriter](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/RowWriter.scala) that uses the ML9 [WriteHostBatcher](https://github.com/marklogic/data-movement/blob/develop/data-movement/src/main/java/com/marklogic/datamovement/WriteHostBatcher.java).
3. Note that prototype also demonstrates how to make Spark Connector capabilities available to Spark developer community in a friendly manner. It uses [Scala Implicit Conversions](http://docs.scala-lang.org/tutorials/tour/implicit-conversions) to extend the capabilities of standard Spark classes like *SparkContext*, *DataFrame* and *RDD* via [SparkContextFunctions](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/SparkContextFunctions.scala), [DataFrameFunctions](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/DataFrameFunctions.scala) and [RDDFunctions](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/RDDFunctions.scala). Simplicity from the developer standpoint is demonstrated in examples that use Spark Connector like [RDBBatchDataMover](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/sparkexamples/RDBBatchDataMover.scala) or [MarkLogicSparkAnalytics](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/sparkexamples/MarkLogicSparkAnalytics.scala).     

### How to use MarkLogic Spark Connector ###

#### Prerequisites ####

1. You have [MarkLogic 9 EA3 or above release](http://ea.marklogic.com/) installed and running.
2. JDK 1.8 or above
3. Scala 2.10.x (not 2.11)
4. Spark 1.6 (or above)
5. sbt (Simple Build Tool)   

#### Build ####

Clone this repository and run 

	sbt package

This will download all the dependencies from public repository and create the file /target/scala-2.10/marklogic-spark-connector_2.10-1.0.0-SNAPSHOT.jar.

#### Setup ####

1. Configure MarkLogic cluster, created a database and design the forest layout across multiple hosts. 
2. Using mlcp, load a relatively large number of documents and tag documents with one or more collection names such that a collection query will typically generate results from multiple forests across multiple hosts.
3. Optionally setup a REST server for the database. You can use the default REST server at port 8000 as well.
4. To execute RDB2MarkLogicDataMover, you will need have an Oracle instance setup and change the code to fetch the data from the Oracle table/view. 

#### Usage ####

Navigate to the /target/scala-2.10 directory. Make sure all the dependent libraries are available in lib sub directory. 

	spark-submit --jars ./lib/commons-codec-1.4.jar,./lib/commons-logging-1.1.1.jar, \
						./lib/data-movement-1.0.0-EA3.jar,./lib/httpclient-4.1.1.jar, \
						./lib/httpcore-4.1.jar,./lib/jackson-annotations-2.4.1.jar, \
						./lib/jackson-core-2.4.1.jar,./lib/jackson-databind-2.4.1.jar, \
						./lib/java-client-api-4.0.0-EA3.jar,./lib/jersey-apache-client4-1.17.jar, \
						./lib/jersey-client-1.17.jar,./lib/jersey-core-1.17.jar, \
						./lib/jersey-multipart-1.17.jar,./lib/mimepull-1.6.jar,./lib/mimepull-1.9.4.jar \
						,./lib/ojdbc6-11.2.0.2.jar,./lib/slf4j-api-1.7.4.jar \
						--class com.marklogic.sparkexamples.RDB2MarkLogicDataMover \
						--master local[2] ./marklogic-spark-connector_2.10-1.0.0-SNAPSHOT.jar
						

To run MarkLogicDataAnalyzer, change the --class parameter to com.marklogic.sparkexamples.MarkLogicDataAnalyzer.

## Design Issues, Concerns, Next Steps##
1. Example applications have the hard coded values of parameters. The developer should be able to pass the values either via command line or via a file.
2. The unity testing of [MarkLogicDocumentRDD](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/spark/DocumentRDD.scala) code via [DocumentRDDTest](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/test/scala/com/marklogic/spark/DocumentRDDTest.scala) succeeds but when you run [MarkLogicDataAnalyzer](https://github.com/HemantPuranik/MarkLogic-Spark-Connector/blob/master/src/main/scala/com/marklogic/sparkexamples/MarkLogicSparkAnalytics.scala) via spark-submit commands, it fails because Spark uber jar contains older version of jersey-core and conflicts with what ML9 Java Client needs. The possible work around is to use latest version of Spark which uses recent version of jersey-core or configure Spark to load user specified jars. The later is experimental feature of Spark at this time.
3. Given the recent trend of innovations within Spark community around DataFrame and Machine Learning which is also complementary to MarkLogic capabilities, maybe there is more value in making the rows from row index available within Spark ecosystem. Although prototype does not address how to load rows from MarkLogic into a DataFrame, how to partition those rows etc. Maybe this needs to be addressed in QueryHostBatcher first before addressing it in Spark Connector. 
4. The protytype was done using Spark 1.6 and does not take into account any changes, improvements to Spark in Spark 2.x. 
5. The overall code is a prototype quality as author is new to Scala programming.         
