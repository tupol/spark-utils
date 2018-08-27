
name := "spark-utils"

organization := "org.tupol"

scalaVersion := "2.11.12"

val scalaUtilsVersion = "0.1.0"

val sparkVersion = "2.1.1"

// ------------------------------
// DEPENDENCIES AND RESOLVERS

updateOptions := updateOptions.value.withCachedResolution(true)

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion force(),
  "org.apache.spark" %% "spark-sql" % sparkVersion force(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion force(),
  "org.apache.spark" %% "spark-streaming" % sparkVersion force()
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

libraryDependencies ++= Seq(
  "org.tupol" %% "scala-utils" % scalaUtilsVersion,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
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

pomIncludeRepository := { x => false }


licenses := Seq("MIT-style" -> url("https://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/tupol"))

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

