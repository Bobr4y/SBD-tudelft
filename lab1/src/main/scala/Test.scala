import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}

object Test {
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  case class result (
    topic: String,
    count: Int
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

    // Load all GDELT .csv files into an rdd
    val rdd = sc.textFile("./data/segment/*.csv")

    // AllNames contains all topics and their counts
    //val allNames = rdd
    //  .filter(line => line.split("\t").length > 23) // Only consider lines that have an AllNames column (index 23)
    //  .map(_.split("\t")(23)) // Retrieve the AllNames column




    val allNames = rdd
      .filter(line => line.split("\t").length > 23)
      .map({case(str) => (str.split("\t")(1), str.split("\t")(23))})
      .reduceByKey(_ + _)
      .map({case(date, topics) => (date, topics.split(";")
        .map({case(str) => (str.split(","))})
      )})

    allNames.take(2)



    // =====================================================
    // DIT IS WAT WAARMEE IK TOT NU TOE HET VERST BEN GEKOMEN
    // Je kan de list van topics per datum alleen niet meer reducen omdat het geen rdd meer is?
    val topicsByDate = rdd
      .filter(line => line.split("\t").length > 23)
      .map({case(str) => str.split("\t")(1) -> str.split("\t")(23)})
      .reduceByKey(_ + _)

    val singleTopicsByDate = topicsByDate
      .flatMapValues(topics => topics.split(";"))
      .filter(str => str._2.split(",").length > 1)
      .filter(str => !(excludes.contains(str._2.split(",")(0))) )
      .filter(str => toInt(str._2.split(",")(1)) != None)
      .mapValues(x => x.split(",")(0) -> x.split(",")(1).toInt)
      .groupByKey()

    // ======================================================
    // ======================================================

    val topTenByDate = singleTopicsByDate
      .map({
        case(date, list) => date -> list
      })

    val separateTopicsByDate = topicsByDate
      .mapValues(_.split(";").map({case(str) => (str)}))
      .mapValues({case(arr) => arr
        .filter(str => str.split(",").length > 1)
        .filter(str => !(excludes.contains(str.split(",")(0))) )
        .filter(str => toInt(str.split(",")(1)) != None)
        .map({case(str) => (str.split(",")(0), str.split(",")(1).toInt)})
      })

      separateTopicsByDate.take(2)

    val test = separateTopicsByDate.map({ 
      case (key, values) => (key, values.groupBy(identity).reduceByKey(_ + _)) 
    })

    test.take(2)






    val topicsByDate = rdd
      .filter(line => line.split("\t").length > 23)
      .map({case(str) => (str.split("\t")(1), str.split("\t")(23))})
      .reduceByKey(_ + _)

    val singleTopicsByDate = topicsByDate.mapValues(_.split(";")
      .flatMap({case(str) => (str)})
    )

    singleTopicsByDate.take(2)









    val topicsByDate = allNames.reduceByKey(_ + _)

    val topicByDate = topicsByDate.map({
      case(date, topics) => (date, topics
        .split(";")
        .filter(str => str.split(",").length > 1)
        .filter(str => !(excludes.contains(str.split(",")(0))) )
        .filter(str => toInt(str.split(",")(1)) != None)
        .map({case(str) => (str.split(",")(0), str.split(",")(1).toInt)})
      )
    })

    val topTen = topicByDate.map({
      case(date, topics) => (date, sc.parallelize(topics).reduceByKey(_ + _))
    })

    // Names countains a list of strings with a single topic and its count in the string
    val names = allNames.flatMap(line => line.split(";")) // Separate topics from allNames to get a single topic and its count

    // Topics splits the string into a topic name part and a count part
    val topics = names
      .filter(str => str.split(",").length > 1) // Only consider strings that contain a topic and a count
      .filter(str => !(excludes.contains(str.split(",")(0))) ) // Exclude topics from the excludes list
      .map({case(str) => (str.split(",")(0), str.split(",")(1).toInt)}) // Put the topic and the count into a map

    // Reduce the topics by key (topics) and add the values (counts)
    val reducedTopics = topics.reduceByKey(_ + _)

    // Sort by descending values (counts)
    val sortedTopics = reducedTopics.sortBy(_._2, false)

    sortedTopics.take(10).foreach(println)

    spark.stop()
  }
}
