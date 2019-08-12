import org.apache.spark.SparkContext

object LearScala {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "LearningSpark")
    val lines = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/ratings.csv")
    val ratings = lines.map(x => x.split(",")(2))
    val m = ratings.countByValue()
    println(m)
  }
}
