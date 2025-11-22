object FlinkIcebergProcessor{
        def main(args:Array[String]):Unit={

        val settings=EnvironmentSettings.newInstance().inStreamingMode().build()
        val tableEnv=TableEnvironment.create(settings)

        // Configuration pour Nessie/Iceberg
        val config=new Configuration()
        config.setString("catalog-name","nessie")
        config.setString("catalog-type","nessie")
        config.setString("uri","http://localhost:19120/api/v1")
        config.setString("warehouse","s3a://data/demo")
        config.setString("ref","main")

        // Configuration MinIO
        config.setString("s3.endpoint","http://localhost:9000")
        config.setString("s3.access-key","admin")
        config.setString("s3.secret-key","password")
        config.setBoolean("s3.path-style-access",true)

        // Créer une table temporaire pour lire depuis Kafka
        tableEnv.executeSql("""
      CREATE TABLE kafka_ventes (
        IDVente STRING,
        DateVente STRING,
        Produit STRING,
        Categorie STRING,
        Quantite INT,
        PrixUnitaire DOUBLE,
        Client STRING,
        Ville STRING,
        Pays STRING,
        Remise DOUBLE,
        MoyenPaiement STRING,
        Total DOUBLE
      ) WITH (
        'connector' = 'kafka',
        'topic' = 'quickstart-eventsout',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-iceberg',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
      )
    """)

        // Insérer les données dans Iceberg
        tableEnv.executeSql("""
      INSERT INTO nessie.demo.ventes
      SELECT
        CAST(IDVente AS BIGINT) as id,
        Produit as produit,
        PrixUnitaire as prixUnitaire,
        Quantite as quantite,
        Total as total,
        Remise as remise,
        Ville as ville,
        Client as client,
        MoyenPaiement as moyenPaiement,
        CAST(DateVente AS DATE) as dateVente
      FROM kafka_ventes
      WHERE IDVente IS NOT NULL
    """)

        println("✅ Job Flink-Iceberg démarré avec succès!")
        }
        }
