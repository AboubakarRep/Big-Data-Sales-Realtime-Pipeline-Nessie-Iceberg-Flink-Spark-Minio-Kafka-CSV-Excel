import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.minio.MinioClient
import io.minio.messages.Item
import java.time.{ZonedDateTime, ZoneOffset}
import scala.collection.JavaConverters._ // ✅ Scala 2.12 compatible

object MinioLatestToKafka1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MinioLatestToKafka")
      .master("local[*]")
      .getOrCreate()

    // Config S3A pour Spark
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000")
    hadoopConf.set("fs.s3a.access.key", "admin")
    hadoopConf.set("fs.s3a.secret.key", "password")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // Initialiser le client MinIO
    val minioClient = new MinioClient("http://localhost:9000", "admin", "password")

    val bucketName = "data"
    val objects = minioClient.listObjects(bucketName).iterator().asScala

    var latestFile: String = ""
    var latestModified = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

    for (obj <- objects) {
      val item: Item = obj.get()
      val itemDate = item.lastModified().toInstant.atZone(ZoneOffset.UTC) // ✅ conversion Date → ZonedDateTime
      if (itemDate.isAfter(latestModified)) {
        latestModified = itemDate
        latestFile = item.objectName()
      }
    }

    if (latestFile.isEmpty) {
      println("Aucun fichier trouvé dans le bucket")
      System.exit(1)
    }

    println(s"Traitement du fichier le plus récent: $latestFile")

    val filePath = s"s3a://$bucketName/$latestFile"

    val schema = StructType(Seq(
      StructField("IDVente", StringType),
      StructField("DateVente", StringType),
      StructField("Produit", StringType),
      StructField("Categorie", StringType),
      StructField("Quantité", IntegerType),
      StructField("PrixUnitaire", IntegerType),
      StructField("Client", StringType),
      StructField("Ville", StringType),
      StructField("Pays", StringType),
      StructField("Remise", DoubleType),
      StructField("MoyenPaiement", StringType),
      StructField("Total", DoubleType)
    ))

    val data = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(filePath)

    data.printSchema()
    data.show(false)

    data.select(to_json(struct(data.columns.map(col): _*)).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "quickstart-eventsin")
      .save()

    println(s"Nombre de lignes lues : ${data.count()}")
    data.limit(5).show(false)

    spark.stop()
  }
}
