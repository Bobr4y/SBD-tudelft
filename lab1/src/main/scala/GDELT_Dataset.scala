package GDELT_Dataset

import org.apache.spark.sql.SparkSession
import scala.util.Sorting.stableSort
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}

object TopTenTopics {

  // topicsByDate case class
  case class topicsByDate (
      date: Timestamp,
      allNames: String
  )

  // The result case class
  case class Result (
      date: Timestamp,
      topics: List[(String, BigInt)]
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

    // Dataframe with [date, topic, count]
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd") // Date formatter
    val singleTopicsByDate = ds
        .filter(x => x.date != null)  // Check that date value is not null
        .filter(x => x.allNames != null)  // Check that allNames value is not null
        .flatMap(x => {
            val date = format.format(x.date)
            val topics = x.allNames.split(";")
            topics
              .filter(x => !(excludes.contains(x.split(",")(0)))) // Check that topic is not in excludes list
              .map(a => (date, a.split(",")(0), a.split(",")(1).toInt)) // Map to [date, topic, count]
        })
        .toDF("date", "topic", "count") // Name the columns of the dataframe

    // Group the topics by date and sort by descending count
    val sorted = singleTopicsByDate
      .groupBy('date, 'topic) // First group by date, then group by topic
      .agg(sum('count)) // Aggregate by summing the count values
      .toDF("date", "topic", "count") // Name the columns of the dataframe
      .sort($"count".desc)  // Sort by desc count

    // The result dataset [date, List[topic, count]]
    val result = sorted
      .groupBy('date) // Group by date
      .agg(collect_list(struct('topic, 'count)))  // Aggregate by collecting a list of [topic, count]
      .toDF("date", "topics") // Name the columns of the dataframe
      .as[Result] // As case class Result
      .map(x => {
        val date = format.format(x.date)
        val topicList = x.topics
        (date, topicList.slice(0,10)) // Take the first 10 tuples of the topicList
      })

    // Print loop to display results
    // result.collect.foreach(a => {
    //   println(a._1)
    //   a._2.foreach(println)
    //   println("")
    // })
    result
      .repartition(1)
      .write
      .json("./data/result/test.json")

    spark.stop()
  }
}
