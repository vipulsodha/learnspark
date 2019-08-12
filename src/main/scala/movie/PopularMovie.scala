package movie

import java.nio.charset.CodingErrorAction

import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object PopularMovie {

  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/ml-100k/u.item").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "PopularMovie")

    val nameDict = sc.broadcast(loadMovieNames)

    val file = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/ml-100k/u.data")
    val m = file.map(x => (x.split("\t")(1).toInt, 1)).reduceByKey((x, y) => x+y).map(x => x.swap).sortByKey().map(x => (nameDict.value(x._2), x._1)).collect()

    m.foreach(println)







  }
}
