package fakeFriends

import org.apache.spark.SparkContext

object FakeFriends {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "FakeFriends")
    val friends = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/fakefriends.csv")
    val ageFriends = friends.map(x => x.split(",")).map(x => (x(2).toInt, x(3).toInt)).mapValues(x => {
      (x,1)
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1/x._2).sortByKey().collect()
    ageFriends.iterator.foreach((x) => println(x))

  }
}
