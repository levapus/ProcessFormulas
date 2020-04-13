name := "HelloGraphFrames"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-graphx" % "2.4.5"
)

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "graphframes" % "graphframes" % "0.8.0-spark3.0-s_2.12"

