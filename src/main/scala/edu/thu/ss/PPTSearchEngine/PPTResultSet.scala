package edu.thu.ss.PPTSearchEngine

import org.apache.spark.SparkContext
import edu.thu.ss.PPTSearchEngine.Properties.Config
import edu.thu.ss.PPTSearchEngine.Cluster.PPTCluster
import org.apache.spark.rdd.RDD

/*
 * Format of Web Data:
 * Delimiter: vertical bar  "|"
 * DocId(1) | Title(2) | Category(3) | Url(4) | Date(5) | Words(6)|
 * 
 * Words are a list of words appearing in the web page with whitespace as delimiter
 * 
 */
class PPTResultDocument(docId: Int, query: String, labels: Array[String], data: Array[Array[String]]) extends java.io.Serializable {
  println(docId)
  val label = labels(0)
  val line = data.filter(line => line.head.equals(docId.toString)).head
  val Title: String = line(1)
  val DocId: String = line(0)
  val Url: String = line(3)
  //(year, month, dya)
  val Date: (Int, Int, Int) = {
    val date = line(4).split("-")
    (date(0).toInt, date(1).toInt, date(2).toInt)
  }
  val document = line.last.split(" ")

  val KeyWordIndex = query.split(" ").map { keyword => document.indexOf(keyword) }.filter(index => index != -1).toSet

  val offset = 150 / KeyWordIndex.size / 2

  val Abstract: String = {
    val index = KeyWordIndex.map { index => (index - offset to index + offset).toArray }.fold(Array[Int]())((a, b) => a ++ b)
      .filter(number => number >= 0 && number < document.length).toSet.toList.sorted.take(140)

    def insertDot(index: List[Int]): List[Int] = {
      index match {
        case n1 :: n2 :: ns => if (n2 - n1 > 2) n1 :: (-1) :: insertDot(n2 :: ns) else n1 :: insertDot(n2 :: ns)
        case ns             => ns
      }
    }

    insertDot(index).map { number => if (number == -1) "..." else document(number) }.fold("")((a, b) => a ++ b)
  }
}

class PPTResultSet(docIds: Array[Int], query: String, k: Int, sc: SparkContext) extends java.io.Serializable {
  val RetrievalDoc: Array[PPTResultDocument] = {
    if (docIds.length == 0) {
      Array()
    } else {
      val cluster = new PPTCluster(docIds, sc)
      val result = cluster.KMeans(k, 3)
      val data = sc.textFile(Config.getString("dataDir"))
        .filter { line => docIds.contains(line.split('|').head.toInt) }
        .map { line => line.split('|') }.collect()
      result.map {
        case (docId, label) =>
          println(label)
          new PPTResultDocument(docId, query, Array(label), data)
      }
    }
  }
}