import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaToMinio3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToMinio")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // === Configuration MinIO ===
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "admin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "password")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // === Schéma utilisé pour parser les messages Kafka (correspond à tes colonnes renommées) ===
    val schema = StructType(Seq(
      StructField("IDVente", StringType),
      StructField("DateVente", StringType),
      StructField("Produit", StringType),
      StructField("Categorie", StringType),
      StructField("Quantité", StringType),
      StructField("PrixUnitaire", StringType),
      StructField("Client", StringType),
      StructField("Ville", StringType),
      StructField("Pays", StringType),
      StructField("Remise", StringType),
      StructField("MoyenPaiement", StringType),
      StructField("Total", StringType)
    ))

    // === Lecture depuis Kafka topic `quickstart-eventsout` ===
    val kafkaData = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-eventsout")
      .option("startingOffsets", "earliest")
      .load()

    /*
    // === Parser les messages JSON en DataFrame structuré ===
    val parsedData = kafkaData
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).alias("data"))
      .select("data.*")

    /*
    ////////////////////////////////////////////////////////////////////////////////////////

      // === Dossier temporaire d'écriture (avec coalesce pour 1 fichier) ===
      val tempOutputDir = "s3a://data/temp_output_parquet"

      parsedData.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(tempOutputDir)

      // === Renommer le fichier part-0000...parquet ===
      val fs = FileSystem.get(new URI("s3a://data"), spark.sparkContext.hadoopConfiguration)

      val files: Array[FileStatus] = fs.listStatus(new Path(tempOutputDir))

      val partFileOption = files.map(_.getPath).find(_.getName.startsWith("part-"))

      partFileOption match {
        case Some(partFile) =>
          val newFileName = s"ventes_${System.currentTimeMillis()}.parquet"
          val newFilePath = new Path(s"s3a://data/$newFileName")

          if (fs.exists(newFilePath)) fs.delete(newFilePath, false)
          fs.rename(partFile, newFilePath)

          // Supprimer le dossier temporaire
          fs.delete(new Path(tempOutputDir), true)

          println(s"✅ Fichier renommé et déplacé vers : $newFilePath")

          // === Lire et afficher le fichier parquet renommé ===
          val dfRead = spark.read.parquet(newFilePath.toString)
          dfRead.printSchema()
          dfRead.show(false)

        case None =>
          println("⚠️ Aucun fichier part-xxxx.parquet trouvé dans le dossier temporaire")
      }

     // spark.stop()

      ///////////////////////////////////////////////////////////////////////////////////////



 */
    println("=== Affichage des premiers messages bruts Kafka ===")
    kafkaData.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset")
      .show(10, truncate = false)

    // === Parsing JSON dans un DataFrame structuré ===
    val parsedData = kafkaData
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).alias("data"))
      .select("data.*")

    println("=== Affichage du DataFrame après parsing JSON ===")
    parsedData.show(10, truncate = false)
    parsedData.printSchema()

    // === Génération du chemin de sortie MinIO ===
    val outputPath = s"s3a://data/output_${System.currentTimeMillis()}.parquet"

    try {
      // === Écriture au format Parquet dans MinIO ===
      parsedData.write
        .mode("overwrite") // ou "append" selon besoin
        .parquet(outputPath)

      println(s"✅ Données écrites avec succès dans MinIO à l'emplacement : $outputPath")

      // === Relecture pour vérification ===
      val parquetDF = spark.read.parquet(outputPath)
      println("=== Contenu lu depuis Parquet MinIO ===")
      parquetDF.show(10, truncate = false)
      parquetDF.printSchema()

    } catch {
      case e: Exception =>
        println(s"❌ Erreur lors de l'écriture/lecture dans MinIO : ${e.getMessage}")
        e.printStackTrace()
    }

    // === Pour debug : Affichage avant fin ===
    println("=== Affichage final du DataFrame structuré ===")
    parsedData.show(10, truncate = false)



     */



    println("=== Affichage des premiers messages bruts Kafka ===")
    kafkaData.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset")
      .show(10, truncate = false)

    // === Parsing JSON des messages Kafka ===
    val parsedData = kafkaData
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).alias("data"))
      .select("data.*")

    println("=== Affichage du DataFrame après parsing JSON ===")
    parsedData.show(10, truncate = false)
    parsedData.printSchema()

    // === Filtrage des lignes vides (IDVente null) pour ne garder que les données valides ===
    val parsedDataFiltered = parsedData.filter(col("IDVente").isNotNull)

    println("=== DataFrame après filtrage des lignes avec IDVente non null ===")
    parsedDataFiltered.show(10, truncate = false)

    // === Génération du chemin de sortie dans MinIO ===
    val outputPath = s"s3a://data/output_${System.currentTimeMillis()}.parquet"

    try {
      // === Écriture au format Parquet dans MinIO ===
      parsedDataFiltered.write
        .mode("overwrite") // ou "append" selon besoin
        .parquet(outputPath)

      println(s"✅ Données écrites avec succès dans MinIO à l'emplacement : $outputPath")

      // === Relecture pour vérification ===
      val parquetDF = spark.read.parquet(outputPath)
      println("=== Contenu lu depuis Parquet MinIO ===")
      //parquetDF.show(10, truncate = false)
      parquetDF.show()
      parquetDF.printSchema()

    } catch {
      case e: Exception =>
        println(s"❌ Erreur lors de l'écriture/lecture dans MinIO : ${e.getMessage}")
        e.printStackTrace()
    }

  }

}
