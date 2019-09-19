import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Test {
  def main(args: Array[String]) {
    // Set log level to error to avoid all info logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Start spark session
    val spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Define topics to exclude from the results
    val exclude = List(
      "Type ParentCategory",
      "CategoryType ParentCategory",
      "Comment Tag",
      "View Variables Category Comment"
    )

    // Load all GDELT .csv files into an rdd
    val rdd = sc.textFile("./data/segment/*.csv")

    // AllNames contains all topics and their counts
    val allNames = rdd
      .filter(line => line.split("\t").length > 23) // Only consider lines that have an AllNames column (index 23)
      .map(_.split("\t")(23)) // Retrieve the AllNames column

    // Names countains a list of strings with a single topic and its count in the string
    val names = allNames.flatMap(line => line.split(";")) // Separate topics from allNames to get a single topic and its count

    // Topics splits the string into a topic name part and a count part
    val topics = names
      .filter(str => str.split(",").length > 1) // Only consider strings that contain a topic and a count
      .filter(str => !(exclude.contains(str.split(",")(0))) )
      .map({case(str) => (str.split(",")(0), str.split(",")(1).toInt)}) // Put the topic and the count into a map

    // Reduce the topics by key (topics) and add the values (counts)
    val reducedTopics = topics.reduceByKey(_ + _)

    // Sort by descending values (counts)
    val sortedTopics = reducedTopics.sortBy(_._2, false)

    sortedTopics.take(10).foreach(println)

    spark.stop()
  }
}
