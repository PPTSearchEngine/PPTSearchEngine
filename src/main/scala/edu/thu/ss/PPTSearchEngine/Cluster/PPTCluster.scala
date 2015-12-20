package edu.thu.ss.PPTSearchEngine.Cluster

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import edu.thu.ss.PPTSearchEngine.Properties.Config

class PPTCluster {
  val conf = new SparkConf().setAppName("app").setMaster("local")
  val sc = new SparkContext(conf)
  var result: RDD[(Int, Int)] = null
  
  val WordDocumentMatric = sc.textFile(Config.getString("WDMdir")).map { line => {
      val params = line.split(",")
      (params(0), params(1).toInt, params(2).toDouble)
    }}
  
  def KMeans(k: Int, round: Int, docId: Array[Int]) {
    val random = scala.util.Random
    var indexes = Array[Int]() 
    for (i <- 1 to k) {
      val index = random.nextInt(docId.length - 1) 
      indexes = indexes :+ docId(index)
    } 
    var centers = GetClusterCenters(indexes)
    for (i <- 1 to round) {
      centers = KmeansCluster(centers)
    }
  }
  
  //word, docId, frequency
  def GetClusterCenters(centers: Array[Int]) 
  : RDD[(String, Int, Double)] = {
    WordDocumentMatric.filter { case (w,d,f) => centers.contains(d)}
  }
  
  def KmeansCluster(centers: RDD[(String, Int, Double)]) : RDD[(String, Int, Double)] = {
    val WDM = WordDocumentMatric.map{case (w,d,f) => (w,(d,f))}
    val CENTER = centers.map{ case (w,c,f) => (w, (c,f))}
    val joinresult = WDM.join(CENTER).map{ case (w,((d,f1),(c,f2))) => (d,c)->(f1-f2)*(f1-f2)}
      .reduceByKey((a,b)=>a+b).map{case ((d,c),f) => (d,(c,f))}
    val groups = joinresult.reduceByKey((a,b) => {
      val (c1,f1) = a
      val (c2,f2) = b
      if (f1 > f2) b else a
      }).map{ case(d,(c,f)) => (d,c)}
    result = groups.cache()
    
    val newCenters = groups.join(WordDocumentMatric.map{ case (w,d,f) => (d,(w,f))})
    newCenters.map{case (d,(c,(w,f))) => (c,w)->(1,f)}.reduceByKey((a,b) => {
      val (d1,f1) = a
      val (d2,f2) = b
      (d1+d2,f1+f2)
    }).map{case ((c,w),(d,f)) => (w,c,f/d)}
  }
  
  def OutputClusterResult() {
    result.foreach({case (d,c) => println(d+","+c+"\r\n")})
  }
  
  //document->center
  def LabelCluster(clusters: RDD[(Int,Int)]) : RDD[(Int, String)] = {
    val res = clusters.join(WordDocumentMatric.map{ case (w,d,f) => (d,(w,f))}).map{case (d,(c,(w,f))) => ((c,w),f)}
    .reduceByKey((a,b)=>a+b).map{case ((c,w),f) => (c,(w,f))}.reduceByKey((a,b) => {
      val (w1,f1) = a
      val (w2,f2) = b
      if (f1 > f2) a else b
    }).map{case (c,(w,f)) => (c,w)}
    res
  }
  
  def OutputLabel() : Array[(String, Array[Int])] = {
    result.map{ case (d,c) => (c,d)}.join(LabelCluster(result)).map{case (c,(d,l)) => l->Array(d)}
    .reduceByKey((a,b)=>a++:b).collect()
  }
  
  /*
  def main(args: Array[String]) {
    KMeans(10,5000,10)
    OutputClusterResult()
  }
  * 
  */
  
}