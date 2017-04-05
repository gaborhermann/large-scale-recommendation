name := "large-scale-recommendation"

version := "0.1.0"

scalaVersion := "2.11.7"

lazy val sparkVersion = "2.1.0"
lazy val flinkVersion = "1.2.0"

lazy val commonDependencies = Seq(
  "org.slf4j" % "slf4j-api" % "1.7.22"
)

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-ml" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion
)

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies
  )

lazy val flinkAdaptiveRecom = (project in file("flink-adaptive-recom")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= flinkDependencies.map(_ % "provided")
  )


lazy val sparkAdaptiveRecom = (project in file("spark-adaptive-recom")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= sparkDependencies.map(_ % "provided"),

    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

  )

lazy val root = (project in file(".")).
  aggregate(core, flinkAdaptiveRecom, sparkAdaptiveRecom)

lazy val commonSettings = Seq(
  organization := "hu.sztaki.ilab",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)
