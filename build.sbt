import Dependencies._
import sbt.Keys.{fork, resolvers}
import sbt.url
import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, commitNextVersion, commitReleaseVersion, inquireVersions, publishArtifacts, pushChanges, runClean, runTest, setNextVersion, setReleaseVersion, tagRelease}

crossScalaVersions := Nil

lazy val basicSettings = Seq(
  organization := "org.tupol",
  name := "spark-utils",
  scalaVersion := Versions.scala,
  crossScalaVersions := Versions.crossScala,
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    s"-target:jvm-${Versions.jvm}",
  ),
  updateOptions := updateOptions.value.withCachedResolution(true),
  libraryDependencies ++= CoreTestDependencies,
  dependencyOverrides ++= NettyOverrides,
  resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  Test / parallelExecution := false,
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "base/java.lang", "base/java.lang.invoke", "base/java.lang.reflect", "base/java.io", "base/java.net", "base/java.nio",
    "base/java.util", "base/java.util.concurrent", "base/java.util.concurrent.atomic",
    "base/sun.nio.ch", "base/sun.nio.cs", "base/sun.security.action",
    "base/sun.util.calendar", "security.jgss/sun.security.krb5",
  ).map("--add-opens=java." + _ + "=ALL-UNNAMED"),
)

lazy val publishSettings = Seq(
  isSnapshot := version.value.trim.endsWith("SNAPSHOT"),
  // Nexus (see https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html)
  publishTo := {
    val repo = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at repo + "content/repositories/snapshots")
    else Some("releases" at repo + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := true,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
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
  coverageMinimumStmtTotal in Test := 80,
  coverageMinimumBranchTotal in Test := 80,
  coverageFailOnMinimum in Test := true,
  coverageExcludedPackages := "org.apache.spark.ml.param.shared.*;.*BuildInfo.*;org.tupol.spark.Logging.*"
)

val commonSettings = basicSettings ++ coverageSettings ++ publishSettings

lazy val `spark-utils-core` = (project in file("utils-core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "spark-utils-core",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions := Seq[BuildInfoOption](BuildInfoOption.BuildTime, BuildInfoOption.ToMap, BuildInfoOption.ToJson),
    buildInfoPackage := "org.tupol.spark.info",
    libraryDependencies ++= ProvidedSparkCoreDependencies,
    libraryDependencies ++= CoreDependencies,
    Test / publishArtifact := true
  )

lazy val `spark-utils-io` = (project in file("utils-io"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "spark-utils-io",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions := Seq[BuildInfoOption](BuildInfoOption.BuildTime, BuildInfoOption.ToMap, BuildInfoOption.ToJson),
    buildInfoPackage := "org.tupol.spark.io.info",
    libraryDependencies ++= ProvidedSparkCoreDependencies,
    libraryDependencies ++= ProvidedSparkKafkaDependencies,
    libraryDependencies ++= CoreDependencies,
    libraryDependencies ++= IoTestDependencies,
    Test / publishArtifact := true
  )
  .dependsOn(`spark-utils-core` % "test->test;compile->compile")

lazy val `spark-utils-io-pureconfig` = (project in file("utils-io-pureconfig"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "spark-utils-io-pureconfig",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions := Seq[BuildInfoOption](BuildInfoOption.BuildTime, BuildInfoOption.ToMap, BuildInfoOption.ToJson),
    buildInfoPackage := "org.tupol.spark.io.pureconf.info",
    libraryDependencies ++= ProvidedSparkCoreDependencies,
    libraryDependencies ++= ProvidedSparkKafkaDependencies,
    libraryDependencies ++= CoreDependencies,
    libraryDependencies ++= IoPureconfigDependencies,
    libraryDependencies ++= IoTestDependencies,
    Test / publishArtifact := true
  )
  .dependsOn(`spark-utils-io` % "test->test;compile->compile")
