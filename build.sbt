import Dependencies._

lazy val commonSettings = Seq(
  /**
    * This settings is a fix for a huge project like this in coursier.
    */
  coursierMaxIterations := 2000,
  /**
    * @see Issue [https://github.com/coursier/coursier/issues/444].
    */
  classpathTypes += "test-jar",
  parallelExecution in Test := false,
  updateOptions := updateOptions.value.withCachedResolution(false).withLatestSnapshots(true),
  classpathTypes += "test-jar",
  scalaVersion := "2.11.11",
  version := "0.1.0-SNAPSHOT",
  organizationName := "MTA SZTAKI DMS",
  organization := "hu.sztaki.ilab.recom",
  test in assembly := {},
  sources in (Compile,doc) := Seq.empty,
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Test, packageDoc) := false,
  assemblyMergeStrategy in assembly := {
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.rename
    case x if x.endsWith("log4j.properties") => MergeStrategy.rename
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  fork in Test := true,
  javaOptions in Test := Seq(
    s"-Dlog4j.configuration=" +
      s"file:///${baseDirectory.value.getAbsolutePath}/src/test/resources/log4j.properties"
  )
)

lazy val sparkVersion = "2.2.0"
lazy val flinkVersion = "1.3.0"

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies
  )

lazy val flink = (project in file("flink-adaptive-recom")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= flinkDependencies
  )

lazy val spark = (project in file("spark-adaptive-recom")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= sparkDependencies
  )