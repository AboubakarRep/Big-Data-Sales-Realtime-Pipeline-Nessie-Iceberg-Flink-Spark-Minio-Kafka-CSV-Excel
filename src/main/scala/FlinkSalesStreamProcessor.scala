import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode

import java.util.Properties
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Job Flink simplifié pour le traitement des données de vente depuis Kafka
 * Version compatible Java 8 - Sans FileSink
 */
object FlinkSalesStreamProcessor {

  // Case class pour représenter les données de vente
  case class Sale(
                   IDVente: String,
                   DateVente: String,
                   Produit: String,
                   Categorie: String,
                   Quantite: Int,
                   PrixUnitaire: Double,
                   Client: String,
                   Ville: String,
                   Pays: String,
                   Remise: Double,
                   MoyenPaiement: String,
                   Total: Double
                 )

  def main(args: Array[String]): Unit = {

    // Configuration de l'environnement Flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Configuration des checkpoints
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // Stratégie de redémarrage
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000))

    // Configuration Kafka
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "flink-sales-processor")

    // 1. SOURCE KAFKA
    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "quickstart-eventsin",
      new SimpleStringSchema(),
      kafkaProps
    )
    kafkaConsumer.setStartFromEarliest()

    val salesStream: DataStream[String] = env.addSource(kafkaConsumer)

    // 2. TRANSFORMATION
    val processedStream = salesStream
      .filter(_.nonEmpty)
      .map { jsonString =>
        try {
          parseSaleFromJson(jsonString)
        } catch {
          case e: Exception =>
            println(s"❌ Erreur parsing JSON: ${e.getMessage}")
            null
        }
      }
      .filter(_ != null)
      .name("ParseSalesData")


    // 🔥 STATISTIQUES PAR CATÉGORIE
    val categoryStats = processedStream
      .map(sale => (sale.Categorie, sale.Total, 1))
      .keyBy(_._1) // ✅ Groupe par catégorie
      .reduce((a: (String, Double, Int), b: (String, Double, Int)) =>
        (a._1, a._2 + b._2, a._3 + b._3)) // ✅ Agrège avec types explicites
      .map { data: (String, Double, Int) =>
        val (categorie, total, count) = data
        val moyenne = total / count
        s"📊 $categorie: $count ventes, CA: ${"%.2f".format(total)}€, Moyenne: ${"%.2f".format(moyenne)}€"
      }

    categoryStats.print().name("CategoryStats")

    // 🔥 TOP 5 DES VILLES
    val cityStats = processedStream
      .map(sale => (sale.Ville, sale.Total))
      .keyBy(_._1)
      .reduce((a: (String, Double), b: (String, Double)) => (a._1, a._2 + b._2))
      .map { data: (String, Double) =>
        val (ville, total) = data
        s"🏙️  $ville: ${"%.2f".format(total)}€"
      }

    cityStats.print().name("CityStats")

    // 🔥 ALERTES VENTES IMPORTANTES
    val bigSalesAlerts = processedStream
      .filter(_.Total > 1000)
      .map(sale => s"🚨 GROSSE VENTE! ${sale.Produit} - ${sale.Total}€ - ${sale.Client}")

    bigSalesAlerts.print().name("BigSalesAlerts")

    // 3. SORTIE VERS KAFKA
    val kafkaOutput = processedStream
      .map(sale => convertToOutputJson(sale))

    kafkaOutput.addSink(
      new FlinkKafkaProducer[String](
        "quickstart-eventsout",
        new SimpleStringSchema(),
        kafkaProps
      )
    ).name("OutputKafkaSink")

    // 4. AFFICHAGE CONSOLE
    processedStream
      .map(sale => s"✅ Vente ${sale.IDVente}: ${sale.Produit} - ${sale.Total}€ - ${sale.Client}")
      .print()
      .name("ConsoleOutput")

    // Démarrage
    println("🚀 Démarrage du job Flink Sales Stream Processor...")
    println("📊 Lecture depuis Kafka: quickstart-eventsin")
    println("📤 Écriture vers Kafka: quickstart-eventsout")

    env.execute("Flink Sales Stream Processor")
  }

  // Parse JSON en objet Sale
  private def parseSaleFromJson(jsonString: String): Sale = {
    implicit val formats: DefaultFormats = DefaultFormats

    val json = parse(jsonString)

    Sale(
      IDVente = (json \ "IDVente").extract[String],
      DateVente = (json \ "DateVente").extract[String],
      Produit = (json \ "Produit").extract[String],
      Categorie = (json \ "Categorie").extract[String],
      Quantite = (json \ "Quantité").extract[String].toInt,
      PrixUnitaire = (json \ "PrixUnitaire").extract[String].toDouble,
      Client = (json \ "Client").extract[String],
      Ville = (json \ "Ville").extract[String],
      Pays = (json \ "Pays").extract[String],
      Remise = (json \ "Remise").extract[String].toDouble,
      MoyenPaiement = (json \ "MoyenPaiement").extract[String],
      Total = (json \ "Total").extract[String].toDouble
    )
  }

  // Conversion vers JSON de sortie
  private def convertToOutputJson(sale: Sale): String = {
    s"""{
       |"IDVente":"${sale.IDVente}",
       |"DateVente":"${sale.DateVente}",
       |"Produit":"${sale.Produit}",
       |"Categorie":"${sale.Categorie}",
       |"Quantité":${sale.Quantite},
       |"PrixUnitaire":${sale.PrixUnitaire},
       |"Client":"${sale.Client}",
       |"Ville":"${sale.Ville}",
       |"Pays":"${sale.Pays}",
       |"Remise":${sale.Remise},
       |"MoyenPaiement":"${sale.MoyenPaiement}",
       |"Total":${sale.Total}
       |}""".stripMargin.replaceAll("\n", "")
  }
}