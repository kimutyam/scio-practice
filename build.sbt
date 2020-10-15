val scioVersion = "0.9.3"
val beamVersion = "2.23.0"
val circeVersion = "0.13.0"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.1",
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfatal-warnings"
  ),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val dataflow: Project = project
  .in(file("."))
  .settings(
    commonSettings,
    name := "scio-practice",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "com.spotify" %% "scio-bigquery" % scioVersion,
      "com.spotify" %% "scio-bigtable" % scioVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "com.google.cloud" % "google-cloud-logging-logback" % "0.118.2-alpha",
    )
  )
  .enablePlugins(PackPlugin)
  .settings(
    packMain := Map(
      "pubsub2bigquery" -> "mc.code.dataflow.storeDWH.PubSubToBigQuery",
    ),
    // @see https://github.com/spotify/scio/issues/2945
    // 2020-05-05時点でmasterにマージされているので、0.9.1以降で解決している可能性あり
    dependencyOverrides ++= Seq(
      "com.google.guava" % "guava" % "27.0.1-jre"
    )
  )
