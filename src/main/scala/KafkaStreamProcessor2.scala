/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaStreamProcessorDebug {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaStreamProcessorDebug")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("_c0", StringType),
      StructField("_c1", StringType),
      StructField("_c2", StringType),
      StructField("_c3", StringType),
      StructField("_c4", StringType),
      StructField("_c5", StringType),
      StructField("_c6", StringType),
      StructField("_c7", StringType),
      StructField("_c8", StringType),
      StructField("_c9", StringType),
      StructField("_c10", StringType),
      StructField("_c11", StringType)
    ))

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-eventsin")
      .option("startingOffsets", "earliest")
      .load()

    val parsed = kafkaStream.select(
      from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    val filtered = parsed.filter(col("_c0") =!= "IDVente")

    val finalDf = filtered.select(
      col("_c0").alias("IDVente"),
      col("_c1").alias("DateVente"),
      col("_c2").alias("Produit"),
      col("_c3").alias("Categorie"),
      col("_c4").alias("Quantité"),
      col("_c5").alias("PrixUnitaire"),
      col("_c6").alias("Client"),
      col("_c7").alias("Ville"),
      col("_c8").alias("Pays"),
      col("_c9").alias("Remise"),
      col("_c10").alias("MoyenPaiement"),
      col("_c11").alias("Total")
    )

    finalDf.printSchema()

    // Affiche en console pour voir les données entrantes
    val consoleQuery = finalDf.writeStream
      .format("console")
      .option("truncate", false)
      .start()

    consoleQuery.awaitTermination()
  }
}

 */

/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaStreamProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaStreamProcessorDebug")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(Seq(
      StructField("_c0", StringType),
      StructField("_c1", StringType),
      StructField("_c2", StringType),
      StructField("_c3", StringType),
      StructField("_c4", StringType),
      StructField("_c5", StringType),
      StructField("_c6", StringType),
      StructField("_c7", StringType),
      StructField("_c8", StringType),
      StructField("_c9", StringType),
      StructField("_c10", StringType),
      StructField("_c11", StringType)
    ))

    // Lire depuis le topic 'quickstart-eventsin'
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-eventsin")
      .option("startingOffsets", "earliest")
      .load()

    // Parser les données JSON dans le schéma
    val parsed = kafkaStream.select(
      from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    // Enlever l'en-tête s'il est présent
    val filtered = parsed.filter(col("_c0") =!= "IDVente")

    // Renommer les colonnes
    val finalDf = filtered.select(
      col("_c0").alias("IDVente"),
      col("_c1").alias("DateVente"),
      col("_c2").alias("Produit"),
      col("_c3").alias("Categorie"),
      col("_c4").alias("Quantité"),
      col("_c5").alias("PrixUnitaire"),
      col("_c6").alias("Client"),
      col("_c7").alias("Ville"),
      col("_c8").alias("Pays"),
      col("_c9").alias("Remise"),
      col("_c10").alias("MoyenPaiement"),
      col("_c11").alias("Total")
    )

    // === Console: afficher les données entrantes pour debug ===
    val consoleQuery = finalDf.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        println(s"====== Batch $batchId ======")
        batchDF.show(truncate = false)
      }
      .option("checkpointLocation", "output/checkpoint_console")
      .start()

    // === Kafka: écrire vers le topic 'quickstart-eventsout' ===
    val kafkaOutput = finalDf
      .select(to_json(struct(finalDf.columns.map(col): _*)).alias("value"))

    val kafkaQuery = kafkaOutput.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "quickstart-eventsout")
      .option("checkpointLocation", "output/checkpoint_kafka")
      .start()

    // Attend les deux flux
    consoleQuery.awaitTermination()
    kafkaQuery.awaitTermination()
  }
}


 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaStreamProcessor2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaStreamProcessor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Schéma JSON correspondant à la structure des messages Kafka
    val schemaJson = new StructType()
      .add("IDVente", StringType)
      .add("DateVente", StringType)
      .add("Produit", StringType)
      .add("Categorie", StringType)
      .add("Quantité", StringType)
      .add("PrixUnitaire", StringType)
      .add("Client", StringType)
      .add("Ville", StringType)
      .add("Pays", StringType)
      .add("Remise", StringType)
      .add("MoyenPaiement", StringType)
      .add("Total", StringType)

    // Lecture stream depuis Kafka topic 'quickstart-eventsin'
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-eventsin")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")  // ← AJOUTEZ CETTE LIGNE
      .load()

    // Parser la colonne 'value' (bytes) en string, puis JSON selon le schema
    val parsed = kafkaStream.select(
      from_json(col("value").cast("string"), schemaJson).alias("data")
    ).select("data.*")
      // Optionnel : filtrer les lignes mal parsées (null dans IDVente)
      .filter(col("IDVente").isNotNull)

    // Affichage console pour debug (foreachBatch pour contrôler)
    val consoleQuery = parsed.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        println(s"====== Batch $batchId ======")
        batchDF.show(truncate = false)
      }
      .option("checkpointLocation", "output/checkpoint_console")
      .start()

    // Réécriture dans Kafka topic 'quickstart-eventsout' en JSON string
    val kafkaOutput = parsed
      .select(to_json(struct(parsed.columns.map(col): _*)).alias("value"))

    val kafkaQuery = kafkaOutput.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "quickstart-eventsout")
      .option("checkpointLocation", "output/checkpoint_kafka")
      .start()

    // Attente des deux streams
    consoleQuery.awaitTermination()
    kafkaQuery.awaitTermination()
  }
}
