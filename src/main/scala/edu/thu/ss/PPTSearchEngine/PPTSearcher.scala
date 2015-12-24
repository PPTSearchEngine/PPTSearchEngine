package edu.thu.ss.PPTSearchEngine

import edu.thu.ss.PPTSearchEngine.Properties.Config
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.math._
import java.io.File
import collection.mutable.ArrayBuffer
import java.io._
import org.apache.spark.rdd.RDD
import edu.thu.ss.PPTSearchEngine.Cluster.PPTCluster

object PPTSearcher {
  val conf = new SparkConf().setAppName("app").setMaster("local[2]")
     conf.set("spark.driver.allowMultipleContexts" , "true") 
    val sc = new SparkContext(conf)
  val dataDir = Config.getString("dataDir")
  val WDMdir = Config.getString("WDMdir")
  val IDMdir = Config.getString("IDMdir")
  val query = Config.getString("query")
  val docLenDir = Config.getString("docLenDir")
    
  def main(args: Array[String]) {
    val resultDoc = search2("百度 工作")
    println(resultDoc.mkString(","))
  }
  val WDM = sc.textFile(WDMdir).map(line => {
    val param = line.split(",")
    (param(0), param(1).toInt, param(2).toDouble)
  })
  /*
  val IDM = sc.textFile(IDMdir).map(line => {
    val param = line.split(",")
    (param(0), param.tail.map(x => x.toInt))
  })*/

  val docLenSq = sc.textFile(docLenDir).map(line => {
    val param = line.split(",")
    (param(0).toInt, param(1).toDouble)
  })

  def search(query: String): PPTResultSet = {
    //val relatedDoc = search1(query)
    //val resultDoc = searchVSM(query, relatedDoc)
    val resultDoc = search2(query)
    new PPTResultSet(resultDoc, query, 4, sc)
  }
  
  def search2(query: String): Array[Int] = {
    val words = query.split(" ")
    println(words.length)
    val vec = sc.parallelize(query.split(" ")).map(x => (x, 1.0))
    val vecLenSq = vec.map({ case (x, y) => y * y }).sum()
    val cosine = WDM.filter(line => words.contains(line._1)).map({ case (t, d, w) => (t, (d, w)) }).join(vec)
      .map({ case (t, ((d, w), w2)) => (d, (t, w, w2)) }).join(docLenSq)
      .map({ case (d, ((t, w1, w2), docSq)) => (d, w1 * w2 / scala.math.sqrt(vecLenSq * docSq)) }).reduceByKey((a, b) => a + b)
    cosine.sortBy(_._2, false).map(x => x._1.toInt).collect()
  }
  
  //slow because of join and reduceByKey
  def search1(query: String): Array[Int] = {
    val vec = sc.parallelize(query.split(" ")).map(x => (x, 1))
    WDM.map{ case (w,d,f) => (w,(d,f))}.join(vec).map{case (w,((d,f),i)) => (d,f)}.reduceByKey((a,b)=>a+b)
    .join(docLenSq).map{case (d,(f,l)) => (d,f/l)}.sortBy(_._2, false).map{case (d,v)=>d}.collect().take(10)
  }


}