scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.1"
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.9.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"
libraryDependencies += "com.lihaoyi" %% "fansi" % "0.3.1"

libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
testFrameworks += TestFramework("munit.Framework")
