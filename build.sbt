
name := "spark-utils"

organization := "io.tupol"

scalaVersion := "2.11.12"

val sparkVersion = "2.1.1"

val log4jVersion = "2.11.0"

// ------------------------------
// DEPENDENCIES AND RESOLVERS

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion force(),
  "org.apache.spark" %% "spark-sql" % sparkVersion force(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion force(),
  "org.apache.spark" %% "spark-streaming" % sparkVersion force()
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scalaz" %% "scalaz-core" % "7.2.5",
  "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe" % "config" % "1.3.0"
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

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := true

publishMavenStyle := true


pomIncludeRepository := { x => false }

pomExtra := (
  <url>https://github.com/tupol/spark-utils</url>
    <licenses>
      <license>
        <name>MIT-style</name>
        <url>https://opensource.org/licenses/MIT</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:tupol/spark-utils.git</url>
      <connection>scm:git:git@github.com:tupol/spark-utils.git</connection>
    </scm>
    <developers>
      <developer>
        <id>tupol</id>
        <name>Oliver Tupran</name>
        <url>https://github.com/tupol</url>
      </developer>
    </developers>
  )

// ------------------------------
// BUILD-INFO
lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "io.tupol.spark.info"
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

