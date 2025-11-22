import org.apache.spark.sql.SparkSession

object NessieIceberg {
  def main(args: Array[String]): Unit = {
    // Créer la session Spark
    val spark = SparkSession.builder()
      .appName("Nessie-Iceberg MinIO Test")
      .master("local[*]") // Ou ton cluster ici
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
      .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
      .config("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")
      .config("spark.sql.catalog.nessie.ref", "main")
      .config("spark.sql.catalog.nessie.warehouse", "s3a://data/")  // Utilisation de MinIO pour le warehouse
      .getOrCreate()

    // Configuration MinIO pour accéder aux données via S3A
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:17000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // On ne définit pas la région ici pour MinIO ou AWS (pas nécessaire)

    try {
      println("\n=== Test de connexion Nessie/Iceberg ===")

      // Créer un namespace test dans Nessie
      spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")

      // Lister les namespaces existants
      println("\nNamespaces existants:")
      spark.sql("SHOW NAMESPACES IN nessie").show()

      // Créer une table dans le namespace "demo"
      spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.demo.test_table (
          id bigint,
          data string
        ) USING iceberg
      """)

      // Insérer des données dans la table
      println("\nInsertion de données dans la table:")
      spark.sql("""
        INSERT INTO nessie.demo.test_table VALUES
        (1, 'Donnée 1'),
        (2, 'Donnée 2'),
        (3, 'Donnée 3')
      """)

      // Lister les tables dans le namespace 'demo'
      println("\nTables dans le namespace 'demo':")
      spark.sql("SHOW TABLES IN nessie.demo").show()

      // Lire les données de la table 'test_table'
      println("\nLecture des données de la table 'test_table':")
      val df = spark.sql("SELECT * FROM nessie.demo.test_table")
      df.show()

      // Sauvegarde des résultats dans MinIO (au format CSV)
      val minioOutputPath = "s3a://data/output/ventes_test_table/"
      println(s"\n=== Sauvegarde des résultats dans MinIO à $minioOutputPath ===")

      // Vérifie si les données existent déjà à l'emplacement MinIO
      val outputExists = {
        try {
          val outputDf = spark.read.option("header", "true").csv(minioOutputPath)
          true
        } catch {
          case _: Exception => false
        }
      }

      if (outputExists) {
        println(s"Les données existent déjà à l'emplacement: $minioOutputPath. Vous pouvez soit les fusionner, soit utiliser un autre répertoire.")
      } else {
        // Sauvegarde des données dans MinIO
        println(s"Enregistrement des données dans MinIO à : $minioOutputPath")
        df.write
          .mode("overwrite")
          .option("header", "true")
          .csv(minioOutputPath)
        println(s"Données enregistrées dans MinIO à : $minioOutputPath")
      }

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
