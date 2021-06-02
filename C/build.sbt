name := "C"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.flink/flink-core
libraryDependencies += "org.apache.flink" % "flink-core" % "1.13.0"

// https://mvnrepository.com/artifact/org.apache.flink/flink-test-utils
libraryDependencies += "org.apache.flink" %% "flink-test-utils" % "1.13.0" % Test

// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.13.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.flink/flink-clients
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.13.0"

