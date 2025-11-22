import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window

object Main4 {

  def main(args: Array[String]): Unit = {
    // Créer la session Spark avec les configurations Iceberg et Nessie
    val spark = SparkSession.builder()
      .appName("Test MinIO avec Iceberg et Nessie")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
      .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
      .config("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")
      .config("spark.sql.catalog.nessie.ref", "main")
      .config("spark.sql.catalog.nessie.warehouse", "s3a://data/demo")  // Utilisation de MinIO pour le warehouse
      .getOrCreate()

    // Configurer MinIO pour l'accès aux données via S3A
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "admin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "password")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    import spark.implicits._

    try {
      println("\n=== Test de connexion Nessie/Iceberg ===")

      // Créer un namespace 'demo' dans Nessie si nécessaire
      spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")

      // Lister les namespaces existants
      println("\nNamespaces existants:")
      spark.sql("SHOW NAMESPACES IN nessie").show()

      // Créer une table dans le namespace 'demo'
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

      // Lire les données depuis MinIO (s3a://data/JeuDonneesVentes.csv)
      val chemin = "s3a://data/JeuDonneesVentes.csv"
      val df = spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .csv(chemin)

      // Renommer les colonnes pour les faire correspondre à celles de la table Iceberg
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

      // Convertir les colonnes en vérifiant les valeurs invalides
      val dfConverted = dfRenamed
        .withColumn("id", when($"id".rlike("^\\d+$"), $"id").otherwise(null).cast("bigint"))
        .withColumn("prixUnitaire", when($"prixUnitaire".rlike("^\\d*\\.?\\d+$"), $"prixUnitaire").otherwise(null).cast("double"))
        .withColumn("quantite", when($"quantite".rlike("^\\d+$"), $"quantite").otherwise(null).cast("int"))
        .withColumn("total", when($"total".rlike("^\\d*\\.?\\d+$"), $"total").otherwise(null).cast("double"))
        .withColumn("remise", when($"remise".rlike("^\\d*\\.?\\d+$"), $"remise").otherwise(null).cast("double"))
        .withColumn("dateVente", when($"dateVente".rlike("^\\d{4}-\\d{2}-\\d{2}$"), $"dateVente").otherwise(null).cast("date"))

      // Sélectionner les colonnes nécessaires
      val dfFinal = dfConverted.select("id", "produit", "prixUnitaire", "quantite", "total", "remise", "ville", "client", "moyenPaiement", "dateVente")

      // Insérer les données dans la table Iceberg
      println("\nInsertion des données dans la table Iceberg:")
      dfFinal.write
        .mode("append")
        .insertInto("nessie.demo.ventes")

      // Afficher les 5 premières lignes des données insérées
      spark.sql("SELECT * FROM nessie.demo.ventes LIMIT 5").show()

      // Sauvegarde dans MinIO dans le dossier ventes_analyses/
      val minioOutputPath = "s3a://data/output/ventes_analyses/"
      println(s"\n=== Sauvegarde des résultats dans MinIO à $minioOutputPath ===")

      // Sauvegarde des données dans MinIO
      df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(minioOutputPath)

      println(s"Données enregistrées dans MinIO à : $minioOutputPath")

      // Afficher les 5 premières lignes des données
      df.show()

      /*

      // Lire la table 'ventes' d'Iceberg stockée dans MinIO via Nessie
      val dfVentes = spark.table("nessie.demo.ventes")

      // Afficher les premières lignes de la table
      dfVentes.show()


      // Lire les fichiers Parquet directement depuis MinIO
      val dfParquet = spark.read
        .format("parquet")
        .load("s3a://data/demo/ventes/")

      // Afficher les premières lignes des fichiers Parquet
      dfParquet.show()

       */


      println("Warehouse utilisé: " + spark.conf.get("spark.sql.catalog.nessie.warehouse"))

      println("\n✅ Connexion réussie et données envoyées dans MinIO et Nessie !")

    } catch {
      case e: Exception =>
        println("\n❌ Échec de la connexion:")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
