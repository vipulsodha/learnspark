package marvel

import java.nio.charset.CodingErrorAction

import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object marvel {
  def loadHeroNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var heronames:Map[Int, String] = Map()
    val lines = Source.fromFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/Marvel-names.txt").getLines()
    for (line <- lines) {
      val fields = line.split(" \"")
      if (fields.length > 1) {
        heronames += (fields(0).toInt -> fields(1))
      }
    }
    return heronames
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "Marvel")
    val nameDict = sc.broadcast(loadHeroNames)
    val file = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/Marvel-graph.txt")
    val m = file.map(x => x.split(" ")).map(x => (x(0).toInt, x.length - 1)).reduceByKey((x,y) => x+y).map(x => x.swap).sortByKey().collect()
    m.foreach(println)

  }
}
