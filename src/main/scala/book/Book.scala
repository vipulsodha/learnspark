package book

import org.apache.spark.SparkContext

object Book {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "Book")
    val book = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/book.txt")
    val m = book.flatMap(x => x.split("\\W+")).map(x => (x.toLowerCase(), 1)).reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey().collect()
    m.iterator.foreach(x => println(x))


  }
}
