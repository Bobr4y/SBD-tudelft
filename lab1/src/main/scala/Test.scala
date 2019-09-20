import org.apache.spark.sql.SparkSession
import scala.util.Sorting.stableSort

import org.apache.log4j.{Level, Logger}

object Test {
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

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

    // Load all GDELT .csv files into an rdd
    val rdd = sc.textFile("./data/segment/*.csv")

    val topicsByDate = rdd
      .filter(line => line.split("\t").length > 23)
      .map({case(str) => str.split("\t")(1).slice(0, 8) -> str.split("\t")(23)})
      .reduceByKey(_ + _)

    val singleTopicsByDate = topicsByDate.flatMapValues(_.split(";"))

    val date_topicsRDD = singleTopicsByDate
      .filter(str => str._2.split(",").length > 1)
      .filter(str => !(excludes.contains(str._2.split(",")(0))) )
      .filter(x => toInt(x._2.split(",")(1)) != None)
      .map({
        case(date, topicCount) => (date + "_" + topicCount.split(",")(0),topicCount.split(",")(1).toInt)
      })
      .reduceByKey(_ + _)

    val result = date_topicsRDD
      .map({
        case(date_topic, count) => (date_topic.split("_")(0), (date_topic.split("_")(1), count))
      })
      .groupByKey()
      .mapValues(x => stableSort(x.toList, (_._2 > _._2) : ((String,Int),(String,Int)) => Boolean).slice(0,10))

    result.take(3)

    spark.stop()
  }
}
