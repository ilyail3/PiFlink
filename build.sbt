name := "PiFlink"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.zeromq" % "jeromq" % "0.4.2",
  "commons-codec" % "commons-codec" % "1.10",
  "org.apache.flink" %% "flink-streaming-scala" % "1.3.2"
)