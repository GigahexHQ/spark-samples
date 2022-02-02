lazy val buildSettings = Seq(
  organization := "com.gigahex",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.6",
  assemblyJarName in assembly := s"${moduleName.value}.jar",
  assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case "unwanted.txt"                                => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "BUILD" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith ".default" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "class" => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  fork in Test := true
)

lazy val sample = (project in file("."))
  .settings(buildSettings)
  .settings(
    name := "spark-samples",
    moduleName := "spark-samples",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0" ,
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.apache.hadoop" % "hadoop-common" % "3.3.1",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
      "org.typelevel" %% "frameless-dataset"  % "0.10.1"
    )
  )




