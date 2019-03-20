name := "dedup"

version := "0.1"

val SPARK_VERSION = "2.4.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "sonatype releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Will's bintray" at "https://dl.bintray.com/willb/maven/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % "provided"

libraryDependencies += "com.redhat.et" %% "silex" % "0.1.2"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"

libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % Test

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

fork := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "DUMMY.DSA") => MergeStrategy.discard
  case PathList("META-INF", "DUMMY.SF") => MergeStrategy.discard
  case PathList("META-INF", "ECLIPSEF.DSA") => MergeStrategy.discard
  case PathList("META-INF", "ECLIPSEF.SF") => MergeStrategy.discard
  case x => MergeStrategy.first
}
