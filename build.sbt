
name := "spark-utils"

organization := "org.tupol"

scalaVersion := "2.12.12"
crossScalaVersions := Seq("2.12.12")

val scalaUtilsVersion = "1.0.0"

val sparkVersion = "3.0.1"
val sparkXmlVersion = "0.13.0"
val fasterxmlVersion = "2.10.0"
val json4sVersion = "3.6.6"

// ------------------------------
// DEPENDENCIES AND RESOLVERS

updateOptions := updateOptions.value.withCachedResolution(true)
resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion force(),
  "org.apache.spark" %% "spark-sql" % sparkVersion force(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion force(),
  "org.apache.spark" %% "spark-streaming" % sparkVersion force(),
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

// Jackson dependencies over Spark and Kafka Versions can be tricky; for Spark 3.0.x we need this override
dependencyOverrides += "com.fasterxml.jackson.core" %% "jackson-databind" % fasterxmlVersion
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % fasterxmlVersion
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-paranamer" % fasterxmlVersion


libraryDependencies ++= Seq(
  "org.tupol" %% "scala-utils" % scalaUtilsVersion,
  "org.json4s" %% "json4s-core" % json4sVersion % "test",
  "org.json4s" %% "json4s-jackson" % json4sVersion % "test",
  "com.h2database" % "h2" % "1.4.197" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
  "com.h2database" % "h2" % "1.4.197" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % "test",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "test"
)

// ------------------------------
// TESTING
parallelExecution in Test := false

fork in Test := true

publishArtifact in Test := true

// ------------------------------
// TEST COVERAGE
import scoverage.ScoverageKeys._
coverageExcludedPackages := "org.apache.spark.ml.param.shared.*;.*BuildInfo.*;org.tupol.spark.Logging.*"
coverageMinimum := 90
coverageFailOnMinimum := true

// ------------------------------
// PUBLISHING
isSnapshot := version.value.trim.endsWith("SNAPSHOT")

useGpg := true

// Nexus (see https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html)
publishTo := {
  val repo = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at repo + "content/repositories/snapshots")
  else
    Some("releases" at repo + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := true

publishMavenStyle := true

pomIncludeRepository := { _ => false }


licenses := Seq("MIT-style" -> url("https://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/tupol/spark-utils"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/tupol/spark-utils.git"),
    "scm:git@github.com:tupol/spark-utils.git"
  )
)

developers := List(
  Developer(
    id    = "tupol",
    name  = "Tupol",
    email = "tupol.github@gmail.com",
    url   = url("https://github.com/tupol")
  )
)
releasePublishArtifactsAction := PgpKeys.publishSigned.value
import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,          // performs the initial git checks
  tagRelease,
  publishArtifacts,              // checks whether `publishTo` is properly set up
  releaseStepCommand(s"""sonatypeOpen "${organization.value}" "${name.value} v${version.value}""""),
  releaseStepCommand("publishSigned"),
  releaseStepCommand("sonatypeRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges                     // also checks that an upstream branch is properly configured
)

// ------------------------------
// BUILD-INFO
lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "org.tupol.spark.info"
  )

buildInfoKeys ++= Seq[BuildInfoKey](
  resolvers,
  libraryDependencies in Test,
  BuildInfoKey.map(name) { case (k, v) => "project" + k.capitalize -> v.capitalize },
  BuildInfoKey.action("buildTime") {
    System.currentTimeMillis
  } // re-computed each time at compile
)

buildInfoOptions += BuildInfoOption.BuildTime
buildInfoOptions += BuildInfoOption.ToMap
buildInfoOptions += BuildInfoOption.ToJson
