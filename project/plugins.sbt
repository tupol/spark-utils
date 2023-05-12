logLevel := Level.Warn

resolvers += "Sonatype OSS Releases"  at "https://oss.sonatype.org/content/repositories/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

addSbtPlugin("org.xerial.sbt"   % "sbt-sonatype" % "2.0")
addSbtPlugin("com.github.sbt"   % "sbt-release"  % "1.1.0")
addSbtPlugin("com.github.sbt"   % "sbt-pgp" % "2.1.2")

addSbtPlugin("org.scoverage"    % "sbt-scoverage"        % "2.0.9")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"         % "0.14.6")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"        % "0.10.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager"  % "1.3.6")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"         % "2.4.3")
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "1.3.3")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
