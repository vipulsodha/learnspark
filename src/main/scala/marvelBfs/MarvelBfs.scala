package marvelBfs

import org.apache.spark.{Accumulator, SparkContext}
import t.DegreesOfSeparation.BFSData

import scala.collection.mutable.ArrayBuffer

object MarvelBfs {
  var hitCounter:Option[Accumulator[Int]] = None
  val charId = 5306
  val targetCharId = 14

  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)


  def doBfs(Node:BFSNode) = {

  }

  def convertToBFS(line: String): BFSNode = {

    val fields = line.split("\\s+")

    val heroID = fields(0).toInt

    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }

    var color:String = "WHITE"
    var distance:Int = 9999

    if (heroID == charId) {
      color = "GRAY"
      distance = 0
    }

    return (heroID, (connections.toArray, distance, color))
  }


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "MarvelBfs")
    hitCounter = Some(sc.accumulator(0))

    val file = sc.textFile("/Users/vipulsodha/IdeaProjects/LearnSpark/src/main/resources/Marvel-graph.txt")
    val startRdd = file.map(convertToBFS)

    startRdd.








  }
}
