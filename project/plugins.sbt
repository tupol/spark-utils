logLevel := Level.Warn

resolvers += "Sonatype OSS Releases"  at "https://oss.sonatype.org/content/repositories/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

//addSbtPlugin("org.xerial.sbt"   % "sbt-sonatype" % "3.9.13")
addSbtPlugin("com.github.sbt"   % "sbt-release"  % "1.3.0")

addSbtPlugin("org.scoverage"    % "sbt-scoverage"        % "2.0.9")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"         % "0.14.6")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"        % "0.10.0")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"         % "2.4.3")
