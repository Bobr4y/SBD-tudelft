package GDELT_Dataset

import org.apache.spark.sql.SparkSession
import scala.util.Sorting.stableSort
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp
import scala.util.Sorting.stableSort

import org.apache.log4j.{Level, Logger}

object TopTenTopics {

  case class topicsByDate (
      date: Timestamp,
      allNames: String
  )

  case class result (
      date: Timestamp,
      topic: List[List[(String, Int)]]
  )

  def main(args: Array[String]) {
    // Set log level to error to avoid all info logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Start spark session
    val spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Define topics to exclude from the results
    val excludes = List(
      "Type ParentCategory",
      "CategoryType ParentCategory",
      "Comment Tag",
      "View Variables Category Comment"
    )

    // Schema for the .csv GDELT dataset
    val gdeltSchema = StructType(
      Array(
        StructField("GKGRECORDID", StringType),
        StructField("DATE", TimestampType),
        StructField("SourceCollectionIdentifier", IntegerType),
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

    // Load all GDELT .csv files into a dataframe
    val df = spark.read
        .format("csv")
        .option("delimiter", "\t")
        .schema(gdeltSchema)
        .option("timestampFormat", "yyyyMMddHHmmss")
        .load("./data/segment/*.csv")

    // Create temporary view from dataframe
    df.createOrReplaceTempView("gdelt")

    // Select date and AllNames column from gdelt view and store into dataset
    val ds = spark.sql("SELECT DATE,ALLNames FROM gdelt").as[topicsByDate]

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val singleTopicsByDate = ds
        .filter(x => x.date != null)
        .filter(x => x.allNames != null)
        .filter(x => x.allNames.split(";").length > 1)
        .flatMap(x => {
            val date = format.format(x.date)
            val topics = x.allNames.split(";")
            topics.map(a => (date, (a.split(",")(0), a.split(",")(1).toInt)))
        })
        .toDF("date", "topic")

    val result = singleTopicsByDate
        .groupBy("date")
        .agg(collect_list("topic"))
    

    spark.stop()
  }
}
