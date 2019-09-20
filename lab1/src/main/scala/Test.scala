import org.apache.spark.sql.SparkSession
import scala.util.Sorting.stableSort

import org.apache.log4j.{Level, Logger}

object Test {
  // Function used to test if a string can be typecasted to int
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

    // Make an rdd with dates and the AllNames string (date, AllNames_string)
    val topicsByDate = rdd
      .filter(line => line.split("\t").length > 23) // Check if csv line contains the AllNames column
      .map({case(str) => str.split("\t")(1).slice(0, 8) -> str.split("\t")(23)}) // map into a (date, AllNames) tuple
      .reduceByKey(_ + _) // Reduce by key (date)

    // Split AllNames_string to single topics and their count and flatmap to (date, "someTopic,someCount")
    val singleTopicsByDate = topicsByDate.flatMapValues(_.split(";"))

    // Rdd with the date and topic concaternated for reducing by key
    // This results in a topic only being contained once per date
    // ("date_someTopic", "someCount")
    val date_topicsRDD = singleTopicsByDate
      .filter(str => str._2.split(",").length > 1) // Check if there is a count associated with the topic
      .filter(str => !(excludes.contains(str._2.split(",")(0))) ) // Certain topics are excluded
      .filter(x => toInt(x._2.split(",")(1)) != None) // Check if the "count" string can be typecasted to int
      .map({
        case(date, topicCount) => (date + "_" + topicCount.split(",")(0),topicCount.split(",")(1).toInt) // Make the ("date_someTopic", "someCount") tuple
      })
      .reduceByKey(_ + _) // Reduce by key (date_someTopic)

    // Separate topics from their dates and group by key (date)
    // The resulting values (topic, count) are sorted on their count and sliced to a length of 10
    val result = date_topicsRDD
      .map({
        case(date_topic, count) => (date_topic.split("_")(0), (date_topic.split("_")(1), count)) // Split to (date, (someTopic, someCount))
      })
      .groupByKey() // Group by key (date)
      .mapValues(x => stableSort(x.toList, (_._2 > _._2) : ((String,Int),(String,Int)) => Boolean).slice(0,10)) // Sort and slice the list of grouped (someTopic, someCount) tuples

    // Print loop to display results
    result.collect.foreach(a => {
      println(a._1)
      a._2.foreach(println)
      println("")
    })

    spark.stop()
  }
}
