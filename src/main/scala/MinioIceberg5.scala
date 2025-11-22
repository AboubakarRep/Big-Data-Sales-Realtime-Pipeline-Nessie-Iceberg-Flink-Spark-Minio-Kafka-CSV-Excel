import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object MinioIceberg5 {

  def main(args: Array[String]): Unit = {

    // Créer la session Spark avec Iceberg + Nessie
    val spark = SparkSession.builder()
      .appName("Spark + Nessie (sans export CSV MinIO)")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
      .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
      .config("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")
      .config("spark.sql.catalog.nessie.ref", "main")
      .config("spark.sql.catalog.nessie.warehouse", "s3a://data/demo") // warehouse dans MinIO, utilisé par Iceberg
      .getOrCreate()

    // Configuration S3A pour MinIO
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "admin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "password")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    import spark.implicits._

    try {
      println("\n=== Connexion Nessie / Iceberg ===")

      // Créer le namespace s'il n'existe pas
      spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")

      // Créer la table ventes
      spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.demo.ventes (
          id BIGINT,
          produit STRING,
          prixUnitaire DOUBLE,
          quantite INT,
          total DOUBLE,
          remise DOUBLE,
          ville STRING,
          client STRING,
          moyenPaiement STRING,
          dateVente DATE
        ) USING iceberg
      """)

      // Lecture du CSV depuis MinIO
      val df = spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .csv("s3a://data/JeuDonneesVentes.csv")

      // Renommage des colonnes
      val dfRenamed = df
        .withColumnRenamed("IDVente", "id")
        .withColumnRenamed("DateVente", "dateVente")
        .withColumnRenamed("Produit", "produit")
        .withColumnRenamed("Quantité", "quantite")
        .withColumnRenamed("PrixUnitaire", "prixUnitaire")
        .withColumnRenamed("Client", "client")
        .withColumnRenamed("Ville", "ville")
        .withColumnRenamed("MoyenPaiement", "moyenPaiement")
        .withColumnRenamed("Total", "total")
        .withColumnRenamed("Remise", "remise")

      // Nettoyage et typage
      val dfConverted = dfRenamed
        .withColumn("id", when($"id".rlike("^\\d+$"), $"id").otherwise(null).cast("bigint"))
        .withColumn("prixUnitaire", when($"prixUnitaire".rlike("^\\d*\\.?\\d+$"), $"prixUnitaire").otherwise(null).cast("double"))
        .withColumn("quantite", when($"quantite".rlike("^\\d+$"), $"quantite").otherwise(null).cast("int"))
        .withColumn("total", when($"total".rlike("^\\d*\\.?\\d+$"), $"total").otherwise(null).cast("double"))
        .withColumn("remise", when($"remise".rlike("^\\d*\\.?\\d+$"), $"remise").otherwise(null).cast("double"))
        .withColumn("dateVente", when($"dateVente".rlike("^\\d{4}-\\d{2}-\\d{2}$"), $"dateVente").otherwise(null).cast("date"))

      // Sélection finale
      val dfFinal = dfConverted.select("id", "produit", "prixUnitaire", "quantite", "total", "remise", "ville", "client", "moyenPaiement", "dateVente")

      // Insertion dans Nessie Iceberg
      println("\n📥 Insertion dans la table Nessie Iceberg...")
      dfFinal.write
        .mode("append")
        .insertInto("nessie.demo.ventes")

      // Vérification
      println("\n✅ Aperçu des données insérées :")
      spark.sql("SELECT * FROM nessie.demo.ventes LIMIT 5").show()

      println("\n✅ Connexion Nessie + Insertion OK (sans export CSV)")

    } catch {
      case e: Exception =>
        println("\n❌ Erreur pendant l'exécution :")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
