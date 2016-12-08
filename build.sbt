name := "MarkLogic-Spark-Connector"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.5"

scalacOptions ++= Seq("-feature")

packAutoSettings

publishArtifact in Test := true

resolvers += "MarkLogic Releases" at "http://developer.marklogic.com/maven2"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"

libraryDependencies += "com.marklogic" % "java-client-api" % "4.0.0-EA3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0"

libraryDependencies += "com.marklogic" % "data-movement" % "1.0.0-EA3"