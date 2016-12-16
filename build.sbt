name := "dynamodb-demo"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.akka" % "akka-http-experimental_2.11" % "2.4.11"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.66"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
