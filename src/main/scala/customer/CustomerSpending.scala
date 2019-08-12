package customer

import org.apache.spark.SparkContext

object CustomerSpending {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "customer")
    val file = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/customer-orders.csv")
    val res = file.map(x => x.split(",")).map(x => (x(0), x(2).toFloat)).reduceByKey((x,y) => x + y).map(x => (x._2, x._1)).sortByKey().collect()

    res.foreach(x => println(x.swap))


  }
}
