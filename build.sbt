name := "nzq-db"

version := "0.1"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.11",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.11" % Test,
  "com.typesafe.akka" %% "akka-cluster" % "2.5.12",
  "com.typesafe.akka" %% "akka-cluster-tools" % "2.5.5"

)
