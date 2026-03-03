name := "pqg6-flink-processor"
version := "1.0"
scalaVersion := "2.12.18"

val flinkVersion = "1.18.1"
val kafkaConnectorVersion = "3.2.0-1.18"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-kafka" % kafkaConnectorVersion,
  "org.apache.flink" % "flink-connector-base" % flinkVersion,
  "org.apache.flink" % "flink-connector-files" % flinkVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyJarName := "pqg6-flink-processor.jar"
