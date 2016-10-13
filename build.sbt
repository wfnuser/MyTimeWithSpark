name := "simrank"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.0.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.1",
  "org.apache.spark" % "spark-graphx_2.11" % "2.0.1")

resolvers  ++= Seq("Apache Repository" at "https://repository.apache.org/content/repositories/releases",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")