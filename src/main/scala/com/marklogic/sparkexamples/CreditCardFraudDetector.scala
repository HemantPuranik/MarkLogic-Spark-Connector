package com.marklogic.sparkexamples

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}


/**
 * Created by hpuranik on 4/27/2016.
 */
class CreditCardFraudDetector(sparkContext : SparkContext) {

  val sqlContext = new SQLContext(sparkContext)

  val creditRatings = sqlContext.read.format("jdbc").options(
                      Map(
                        "driver" -> "oracle.jdbc.driver.OracleDriver",
                        "url" -> "jdbc:oracle:thin:ADVENTUREWORKS/1234@engrlab-129-226.engrlab.marklogic.com:1521:xe",
                        "dbtable" -> "LOAN$CREDITRATINGS"
                      )).load()

  val creditRatingFeatures = creditRatings.withColumn(
                      "ACCOUNTBALANCE", creditRatings("ACCOUNTBALANCE").cast(DoubleType)).withColumn(
                      "CREDITDURATIONMONTHS", creditRatings("CREDITDURATIONMONTHS").cast(DoubleType)).withColumn(
                      "CREDITHISTORY", creditRatings("CREDITHISTORY").cast(DoubleType)).withColumn(
                      "PURPOSE", creditRatings("PURPOSE").cast(DoubleType)).withColumn(
                      "CREDITAMOUNT", creditRatings("CREDITAMOUNT").cast(DoubleType)).withColumn(
                      "VALUESAVINGSSTOCKS", creditRatings("VALUESAVINGSSTOCKS").cast(DoubleType)).withColumn(
                      "LENGTHOFCURRENTEMPLOYMENT", creditRatings("LENGTHOFCURRENTEMPLOYMENT").cast(DoubleType)).withColumn(
                      "INSTALLMENT", creditRatings("INSTALLMENT").cast(DoubleType)).withColumn(
                      "SEXMARITALSTATUS", creditRatings("SEXMARITALSTATUS").cast(DoubleType)).withColumn(
                      "GUARANTORS", creditRatings("GUARANTORS").cast(DoubleType)).withColumn(
                      "DURATIONATCURRENTADDRESS", creditRatings("DURATIONATCURRENTADDRESS").cast(DoubleType)).withColumn(
                      "MOSTVALUABLEAVAILABLEASSET", creditRatings("MOSTVALUABLEAVAILABLEASSET").cast(DoubleType)).withColumn(
                      "AGE", creditRatings("AGE").cast(DoubleType)).withColumn(
                      "CONCURRENTCREDITS", creditRatings("CONCURRENTCREDITS").cast(DoubleType)).withColumn(
                      "HOUSING", creditRatings("HOUSING").cast(DoubleType)).withColumn(
                      "NUMBEROFCREDITSATTHISBANK", creditRatings("NUMBEROFCREDITSATTHISBANK").cast(DoubleType)).withColumn(
                      "OCCUPATION", creditRatings("OCCUPATION").cast(DoubleType)).withColumn(
                      "NOOFDEPENDENTS", creditRatings("NOOFDEPENDENTS").cast(DoubleType)).withColumn(
                      "TELEPHONE", creditRatings("TELEPHONE").cast(DoubleType)).withColumn(
                      "FOREIGNWORKER", creditRatings("FOREIGNWORKER").cast(DoubleType)).withColumn(
                      "CREDITABILITY", creditRatings("CREDITABILITY").cast(DoubleType))

  val Array(trainingData, testData) =  creditRatingFeatures.randomSplit(Array(0.7, 0.3), 5043)
  val featureColumns = Array( "ACCOUNTBALANCE",
                              "CREDITDURATIONMONTHS",
                              "CREDITHISTORY",
                              "PURPOSE",
                              "CREDITAMOUNT",
                              "VALUESAVINGSSTOCKS",
                              "LENGTHOFCURRENTEMPLOYMENT",
                              "INSTALLMENT",
                              "SEXMARITALSTATUS",
                              "GUARANTORS",
                              "DURATIONATCURRENTADDRESS",
                              "MOSTVALUABLEAVAILABLEASSET",
                              "AGE",
                              "CONCURRENTCREDITS",
                              "HOUSING",
                              "NUMBEROFCREDITSATTHISBANK",
                              "OCCUPATION",
                              "NOOFDEPENDENTS",
                              "TELEPHONE",
                              "FOREIGNWORKER")

  val classColumn = "CREDITABILITY"

  var model : Transformer = null

  def trainModel() {

    //configure the VectorAssembler transformer
    val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")

    //transform training data to feature vectors
    val featureVectors = assembler.transform(trainingData)
    featureVectors.select("CREDITAMOUNT", "features").show()

    //create and configure label indexer in order to transform the credibility/risk field to label index
    val labelIndexer = new StringIndexer().setInputCol(classColumn).setOutputCol("label")

    //transform the training labels into indices like (0, numLabels), ordered by label frequencies.
    val preparedTrainingSet = labelIndexer.fit(featureVectors).transform(featureVectors)
    preparedTrainingSet.select("CREDITAMOUNT", "label").show()

    //create and configure the random forest classifier
    val classifier = new RandomForestClassifier()
                        .setImpurity("gini")
                        .setMaxDepth(3)
                        .setNumTrees(20)
                        .setFeatureSubsetStrategy("auto")
                        .setSeed(5043)

    //create the model based on the training set prepared
    model = classifier.fit(preparedTrainingSet)
  }

  def testModel() {

    //configure the VectorAssembler transformer
    val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")

    //transform training data to feature vectors
    val featureVectors = assembler.transform(testData)
    featureVectors.select("CREDITAMOUNT", "features").show()

    //create and configure label indexer in order to transform the credibility/risk field to label index
    val labelIndexer = new StringIndexer().setInputCol(classColumn).setOutputCol("label")

    //transform the training labels into indices like (0, numLabels), ordered by label frequencies.
    val preparedTestSet = labelIndexer.fit(featureVectors).transform(featureVectors)
    preparedTestSet.select("CREDITAMOUNT", "label").show()

    val predictions = model.transform(preparedTestSet)

    predictions.select("CREDITAMOUNT", "label", "prediction").show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")


    val precision = evaluator.setMetricName("precision").evaluate(predictions)

    val recall = evaluator.setMetricName("recall").evaluate(predictions)

    println("Precision = " + precision + ": Recall = " + recall)
  }

}

object CreditCardRatingClassifier extends App{
  //first you create the spark context within java
  val sparkConf = new SparkConf().setAppName(
                                      "com.marklogic.sparkexamples.CreditCardRatingClassifier").setMaster("local")

  val sparkContext = new SparkContext(sparkConf)

  val creditCardRatingRiskClassifier = new CreditCardFraudDetector(sparkContext)

  creditCardRatingRiskClassifier.trainModel()

  creditCardRatingRiskClassifier.testModel()

}
