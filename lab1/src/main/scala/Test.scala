import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Test {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    // val df = spark.read
    //   .format("csv")
    //   .option("delimiter", "\t")
    //   .schema(schema)
    //   .load("./data/segment/20150218230000.gkg.csv")

    val rdd = sc.textFile("./data/segment/*.csv")

    val allNames = rdd.filter(line => line.split("\t").length > 23)map(_.split("\t")(23))
    val names = allNames.flatMap(line => line.split(";"))
    val topics = names.filter(str => str.split(",").length > 1).map({case(str) => (str.split(",")(0), str.split(",")(1).toInt)})

    val reducedTopics = topics.reduceByKey(_ + _)

    val sortedTopics = reducedTopics.sortBy(_._2, false)

    sortedTopics.take(30).foreach(println)

    spark.stop()
  }
}
