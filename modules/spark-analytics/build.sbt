name := "pqg6-spark-analytics"
version := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyJarName := "pqg6-spark-analytics.jar"
