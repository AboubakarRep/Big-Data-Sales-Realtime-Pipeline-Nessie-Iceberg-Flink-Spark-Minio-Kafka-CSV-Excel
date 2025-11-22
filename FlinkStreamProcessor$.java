object FlinkStreamProcessor{
        def main(args:Array[String]):Unit={

        // Créer l'environnement d'exécution Flink
        val env=StreamExecutionEnvironment.getExecutionEnvironment

        println("🚀 Initialisation du job Flink...")

        // Configuration Kafka Source
        val kafkaSource=KafkaSource.builder[String]()
        .setBootstrapServers("localhost:9092")
        .setTopics("quickstart-eventsout")
        .setGroupId("flink-consumer-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

        // CORRECTION : Avec le bon WatermarkStrategy
        val kafkaStream=env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(), // ✅ Correct
        "Kafka Source"
        )

        // Traitement des données
        val processedStream=kafkaStream
        .map{jsonString=>
        try{
        // Log simple pour debug
        println(s"📥 Message reçu: $jsonString")
        jsonString
        }catch{
        case e:Exception=>
        println(s"❌ Erreur de parsing: ${e.getMessage}")
        s"""{"error": "Parse error", "message": "${e.getMessage}", "raw": "$jsonString"}"""
        }
        }
        .filter(_.nonEmpty)

        // Afficher dans la console pour tester
        processedStream.print()

        // Exécuter le job
        println("✅ Job Flink configuré - Démarrage de l'exécution...")
        env.execute("Flink Kafka Processor")
        }
        }
