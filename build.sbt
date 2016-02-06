assemblyJarName in assembly := "VNXCollector.jar"

name := "VNXCollector"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"

libraryDependencies := {
  libraryDependencies.value ++ Seq(
    "org.scala-lang.modules" %% "scala-xml"       % "1.0.4",
    "com.typesafe.akka"      %% "akka-actor"      % "2.3.10",
    "fr.janalyse"            %% "janalyse-ssh"    % "0.9.19" % "compile",
    "org.slf4j"               % "slf4j-api"       % "1.7.+",
    "ch.qos.logback"          % "logback-classic" % "1.1.2" 
  )
}

