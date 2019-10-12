package GDELT_Dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.sql.expressions.Window



import org.apache.log4j.{Level, Logger}

object TopTenTopics {

  // Case class for topics
  case class topics (
    date: Date,
    topics: String
  )

  def main(args: Array[String]) {
    // Set log level to error to avoid all info logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Start spark session
    val spark = SparkSession.builder.master("local").appName("GdeltAnalysis").getOrCreate()
    //val spark = SparkSession.builder.appName("GdeltAnalysis").getOrCreate() // We don't want the emr cluster to run in local mode
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
        StructField("DATE", DateType),
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
        .option("dateFormat", "yyyyMMddHHmmss")
        .option("mode", "DROPMALFORMED")
        //.read("s3://luppesbucket/data/segment/*.csv")
        .csv("./data/segment/*.csv")

    // Separate the AllNames column into single topics
    val topics = df.select('date, explode(split('AllNames, ";")))
        .as("topics")
        .withColumn("topic", split('col, ",")(0))
        .filter(not(
          col("topic").isin(excludes:_*)
        ))
        .drop('col)

    // Group topics by date and sum the topics
    // Also create a rank column using a window function that orders the count descending per date
    val groupedTopics = topics.groupBy('date, 'topic)
        .agg(count('topic).as("count"))
        .withColumn("rank", rank.over(Window.partitionBy('date).orderBy('count desc)))

    // Take the top ten topics per date by picking the 10 lowest ranks
    val topTen = groupedTopics.where('rank <= 10)
        .drop('rank)
        .groupBy('date)
        .agg(collect_list(struct('topic, 'count)))

    // Save the result to a file
    topTen
      .repartition(1)
      .write
      .json("./data/results/" + java.time.LocalDate.now.toString + "-" + System.currentTimeMillis().toString)

    // Stop the spark session
    spark.stop()
  }
}
