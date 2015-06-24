name := "flume-spark"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.0"


libraryDependencies += "org.apache.flume" % "flume-ng-core" % "1.6.0"
libraryDependencies += "org.apache.flume" % "flume-ng-sdk" % "1.6.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1" % "test"
libraryDependencies += "junit" % "junit" % "4.1" % "test"
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1" % "test" classifier "test"
libraryDependencies += "org.apache.flume" % "flume-ng-sinks" % "1.6.0" % "test"

libraryDependencies += "org.apache.curator" % "curator-test" % "2.8.0" % "test"