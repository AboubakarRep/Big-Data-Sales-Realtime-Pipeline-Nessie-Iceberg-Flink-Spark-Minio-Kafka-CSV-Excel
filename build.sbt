/*
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BigData",

    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core" % "3.4.0",
      "org.apache.spark" %% "spark-sql" % "3.4.0",

      // Iceberg et Nessie
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.4" % "1.3.0",
      "org.projectnessie.nessie-integrations" %% "nessie-spark-extensions-3.4" % "0.73.0",

      // Kafka
      "org.apache.kafka" % "kafka-clients" % "3.4.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.0",

      // MinIO
      "io.minio" % "minio" % "8.5.7",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.787",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",

      // Flink - VERSIONS CORRIGÉES
      "org.apache.flink" % "flink-clients" % "1.17.1",
      "org.apache.flink" % "flink-streaming-java" % "1.17.1",
      "org.apache.flink" % "flink-connector-kafka" % "3.0.2-1.17",
      "org.apache.flink" % "flink-connector-files" % "1.17.1",
      "org.apache.flink" %% "flink-streaming-scala" % "1.15.4",


      // Flink Table API
      "org.apache.flink" % "flink-table-api-java" % "1.16.2",
      "org.apache.flink" % "flink-table-api-java-bridge" % "1.16.2",
      "org.apache.flink" % "flink-table-planner_2.12" % "1.16.2",

      // Iceberg Flink integration
      "org.apache.iceberg" % "iceberg-flink-1.16" % "1.3.0"


    ),

    // Résoudre les conflits
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.14.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.14.2",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.14.2"
    )
  )

*/


ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BigData",

    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core" % "3.4.0"  ,
      "org.apache.spark" %% "spark-sql" % "3.4.0" ,


      // Iceberg et Nessie
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.4" % "1.3.0",
      "org.projectnessie.nessie-integrations" %% "nessie-spark-extensions-3.4" % "0.73.0",
      "org.apache.kafka" % "kafka-clients" % "3.4.0",
      "io.minio" % "minio" % "3.0.4",
      //"io.minio" % "minio" % "8.5.7",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.787",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.0",

      // Flink - VERSIONS VALIDES
      "org.apache.flink" %% "flink-scala" % "1.20.3",
      "org.apache.flink" %% "flink-streaming-scala" % "1.20.3" ,
      "org.apache.flink" % "flink-clients" % "1.20.3",
      "org.apache.flink" % "flink-connector-kafka" % "1.17.2",

      "org.apache.flink" % "flink-connector-files" % "1.20.3"
      ,
      "org.apache.flink" % "flink-parquet" % "1.20.3"
      ,
      "org.apache.flink" % "flink-avro" % "1.20.3"
      ,
      "org.apache.flink" % "flink-json" % "1.20.3"
,


    )
  )


/*
"org.apache.flink" % "flink-clients" % "1.17.1"
,
"org.apache.flink" % "flink-streaming-java" % "1.17.1"
,
"org.apache.flink" % "flink-connector-kafka" % "3.0.2-1.17"
,
"org.apache.flink" % "flink-connector-files" % "1.17.1"
,
"org.apache.flink" %% "flink-streaming-scala" % "1.15.4"
,

// Flink Table API
"org.apache.flink" % "flink-table-api-java" % "1.16.2"
,
"org.apache.flink" % "flink-table-api-java-bridge" % "1.16.2"
,
"org.apache.flink" % "flink-table-planner_2.12" % "1.16.2"
,

// Iceberg Flink integration
"org.apache.iceberg" % "iceberg-flink-1.16" % "1.3.0"*/
