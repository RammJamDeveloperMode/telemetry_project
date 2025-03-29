name := "vehicle-telemetry-processor"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test,
  "org.scalatest" %% "scalatest-matchers-core" % "3.2.15" % Test,
  "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.15" % Test
)

// Configuración de ensamblado
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Configuración de Java
javacOptions ++= Seq("-source", "11", "-target", "11")

// Configuración de Scala
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

// Configuración para tests
Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD") 