import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import java.sql.Timestamp
import java.sql.Date

import org.apache.log4j.{Level, Logger}

object Test {

  case class DateResult (
    Date: String,
    AllNames: String
  )

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val schema = StructType(
      Array(
        StructField("GKGRECORDID", StringType),
        StructField("DATE", StringType),
        StructField("SourceCollectionIdentifier", StringType),
        StructField("SourceCommonName", StringType),
        StructField("DocumentIdentifier", StringType),
        StructField("Counts", StringType),
        StructField("V2Counts", StringType),
        StructField("Themes", StringType),
        StructField("V2Themes", StringType),
        StructField("Locations",StringType),
        StructField("V2Locations",StringType),
        StructField("Persons",StringType),
        StructField("V2Persons",StringType),
        StructField("Organizations",StringType),
        StructField("V2Organizations",StringType),
        StructField("V2Tone", StringType),
        StructField("Dates",StringType),
        StructField("GCAM", StringType),
        StructField("SharingImage", StringType),
        StructField("RelatedImages",StringType),
        StructField("SocialImageEmbeds",StringType),
        StructField("SocialVideoEmbeds",StringType),
        StructField("Quotations", StringType),
        StructField("AllNames", StringType),
        StructField("Amounts",StringType),
        StructField("TranslationInfo",StringType),
        StructField("Extras", StringType)
      )
    )

    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load("./data/segment/20150218230000.gkg.csv")

    df.createOrReplaceTempView("gdelt")

    val dr = spark.sql("SELECT DATE,AllNames FROM gdelt WHERE DATE=\"20150218230000\"").as[DateResult]

    //allNames.take(10).foreach(println)
    //topics.take(3).foreach(a => println(a.AllNames))

    val topicResults = dr.flatMap(x => {
      x.AllNames.split(";")
    })

    val result = topicResults.flatMap(x => {
      val topic = x.split(",")(0)
      val count = x.split(",")(1)
      topic.map( a => (a, count))
    })

    result.take(10).foreach(println)

    // val singleTopics = topics.flatMap( arr => {
    //   val date = arr.Date
    //   val allTopics = arr.AllNames
    //   val topic = allTopics.split(";")
    //   topic.map( a => (date, a))
    // })

    // singleTopics.take(10).foreach(println)

    spark.stop()
  }
}
