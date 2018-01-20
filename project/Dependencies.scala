import sbt._

object Dependencies {
  lazy val hadoopVersion = "2.8.0"
  lazy val sparkVersion = "2.2.1"
  lazy val flinkVersion = "1.3.0"

  lazy val commonDependencies = Seq(
    "org.slf4j" % "slf4j-api" % "1.7.22",
    "org.scalaz" %% "scalaz-core" % "7.2.14",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
    "org.scalactic" %% "scalactic" % "3.0.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )

  lazy val flinkDependencies = Seq(
    "org.apache.flink" %% "flink-ml" % flinkVersion,
    "org.apache.flink" %% "flink-scala" % flinkVersion,
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "org.apache.flink" %% "flink-streaming-java" % flinkVersion
  ).map(_.excludeAll(ExclusionRule("org.apache.hadoop"))) ++ hadoopDependencies

  lazy val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  ).map(_.excludeAll(ExclusionRule("org.apache.hadoop"))) ++ hadoopDependencies

  lazy val hadoopDependencies = Seq(
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion
  )
}