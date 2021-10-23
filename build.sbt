import Dependencies._
import sbt.Keys.{fork, resolvers}
import sbt.url
import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, commitNextVersion, commitReleaseVersion, inquireVersions, publishArtifacts, pushChanges, runClean, runTest, setNextVersion, setReleaseVersion, tagRelease}


lazy val basicSettings = Seq(
  organization := "org.tupol",
  name := "spark-utils",
  scalaVersion := Versions.scala,
  crossScalaVersions := Versions.crossScala,
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-Ywarn-unused-import"
  ),
  updateOptions := updateOptions.value.withCachedResolution(true),
  libraryDependencies ++= CoreTestDependencies,
  dependencyOverrides ++= FasterXmlOverrides,
  resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  parallelExecution in Test := false,
  fork in Test := true
)

lazy val publishSettings = Seq(
  isSnapshot := version.value.trim.endsWith("SNAPSHOT"),
  useGpg := true,
  // Nexus (see https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html)
  publishTo := {
    val repo = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at repo + "content/repositories/snapshots")
    else Some("releases" at repo + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := true,
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  licenses := Seq("MIT-style" -> url("https://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/tupol/spark-utils")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/tupol/spark-utils.git"),
      "scm:git@github.com:tupol/spark-utils.git"
    )
  ),
  developers := List(
    Developer(
      id = "tupol",
      name = "Tupol",
      email = "tupol.github@gmail.com",
      url = url("https://github.com/tupol")
    )
  ),
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
)

lazy val coverageSettings = Seq(
  coverageEnabled in Test := true,
  coverageMinimum in Test := 90,
  coverageFailOnMinimum in Test := true,
  coverageExcludedPackages := "org.apache.spark.ml.param.shared.*;.*BuildInfo.*;org.tupol.spark.Logging.*"
)

val commonSettings = basicSettings ++ coverageSettings ++ publishSettings

lazy val core_utils = (project in file("utils-core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "spark-utils-core",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions := Seq[BuildInfoOption](BuildInfoOption.BuildTime, BuildInfoOption.ToMap, BuildInfoOption.ToJson),
    buildInfoPackage := "org.tupol.spark.info",
    libraryDependencies ++= ProvidedSparkCoreDependencies,
    libraryDependencies ++= CoreDependencies,
    publishArtifact in Test := true
  )

lazy val io_utils = (project in file("utils-io"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "spark-utils-io",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions := Seq[BuildInfoOption](BuildInfoOption.BuildTime, BuildInfoOption.ToMap, BuildInfoOption.ToJson),
    buildInfoPackage := "org.tupol.spark.io.info",
    libraryDependencies ++= ProvidedSparkCoreDependencies,
    libraryDependencies ++= ProvidedSparkKafkaDependencies,
    libraryDependencies ++= IoDependencies,
    libraryDependencies ++= IoTestDependencies,
    publishArtifact in Test := true
  )
  .dependsOn(core_utils % "test->test;compile->compile")

lazy val scala_utils = Project(
  id = "scala-utils",
  base = file(".")
).settings(commonSettings: _*)
  .dependsOn(core_utils % "test->test;compile->compile", io_utils)
  .aggregate(core_utils, io_utils)
