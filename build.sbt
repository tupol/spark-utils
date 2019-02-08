
name := "spark-utils"

organization := "org.tupol"

scalaVersion := "2.11.12"

val scalaUtilsVersion = "0.2.0"

val sparkVersion = "2.3.2"
val sparkXmlVersion = "0.4.1"
val sparkAvroVersion = "4.0.0"

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
  "com.databricks" %% "spark-xml" % sparkXmlVersion,
  "com.databricks" %% "spark-avro" % sparkAvroVersion
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

libraryDependencies ++= Seq(
  "org.tupol" %% "scala-utils" % scalaUtilsVersion,
  "com.h2database" % "h2" % "1.4.197" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
  "com.databricks" %% "spark-xml" % sparkXmlVersion % "test",
  "com.databricks" %% "spark-avro" % sparkAvroVersion % "test"
)
// ------------------------------
// TESTING
parallelExecution in Test := false

fork in Test := true

publishArtifact in Test := true

// ------------------------------
// TEST COVERAGE

scoverage.ScoverageKeys.coverageExcludedPackages := "org.apache.spark.ml.param.shared.*"
scoverage.ScoverageKeys.coverageExcludedFiles := ".*BuildInfo.*"

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
    name  = "Oliver Tupran",
    email = "olivertupran@yahoo.com",
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
