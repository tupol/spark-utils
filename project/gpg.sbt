resolvers += Resolver.url("scalasbt-plugin-releases", new URL("https://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")
