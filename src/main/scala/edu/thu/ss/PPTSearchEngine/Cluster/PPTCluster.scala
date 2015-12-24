package edu.thu.ss.PPTSearchEngine.Cluster

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import edu.thu.ss.PPTSearchEngine.Properties.Config
import edu.thu.ss.PPTSearchEngine.PPTSearcher

class PPTCluster(docIds: Array[Int], sc: SparkContext) extends java.io.Serializable {  
  val WordDocumentMatric = PPTSearcher.WDM.filter{case (w,d,f) => docIds.contains(d)}
  val WDM_wdf = WordDocumentMatric.map{case (w,d,f) => (w,(d,f))}
  val WDM_dwf = WordDocumentMatric.map{ case (w,d,f) => (d,(w,f))}
  def KMeans(k: Int, round: Int) : Array[(Int, String)] = {
    val random = scala.util.Random
    var indexes = Array[Int]() 
    for (i <- 1 to k) {
      val index = random.nextInt(docIds.length - 1) 
      indexes = indexes :+ docIds(index)
    } 
    var centers = WordDocumentMatric.filter { case (w,d,f) => indexes.contains(d)}
    for (i <- 1 to round) {
      centers = KmeansCluster(centers)
    }
    CollectResult(centers)
    
  }
  
  //word, docId, frequency
  def GetClusterCenters(centers: Array[Int]) 
  : RDD[(String, Int, Double)] = {
    WordDocumentMatric.filter { case (w,d,f) => centers.contains(d)}
  }
  
  def KmeansCluster(centers: RDD[(String, Int, Double)]) : RDD[(String, Int, Double)] = {
    
    val CENTER = centers.map{ case (w,c,f) => (w, (c,f))}
    val joinresult = WDM_wdf.join(CENTER).map{ case (w,((d,f1),(c,f2))) => (d,c)->(f1-f2)*(f1-f2)}
      .reduceByKey((a,b)=>a+b)
    val groups = joinresult.map{case ((d,c),f) => (d,(c,f))}
      .reduceByKey((a,b) => {if (a._2 > b._2) b else a})
    
    val newCenters = groups.map{ case(d,(c,f)) => (d,c)}.join(WDM_dwf)
    newCenters.map{case (d,(c,(w,f))) => (c,w)->(1,f)}.reduceByKey((a,b) => {
      (a._1+b._1,a._2+b._2)
    }).map{case ((c,w),(d,f)) => (w,c,f/d)}
  }
  
  def CollectResult(centers: RDD[(String, Int, Double)]) : Array[(Int, String)] = {
    
    val CENTER = centers.map{ case (w,c,f) => (w, (c,f))}
    val joinresult = WDM_wdf.join(CENTER).map{ case (w,((d,f1),(c,f2))) => (d,c)->(f1-f2)*(f1-f2)}
      .reduceByKey((a,b)=>a+b)
    val groups = joinresult.map{case ((d,c),f) => (d,(c,f))}.reduceByKey((a,b) => {     
      if (a._2 > b._2) b else a
      })
    
    val labels = groups.map{ case(d,(c,f)) => (d,c)}.join(WDM_dwf)
    .map{case (d,(c,(w,f))) => ((c,w),f)}
    .reduceByKey((a,b)=>a+b).map{case ((c,w),f) => (c,(w,f))}.reduceByKey((a,b) => {
      if (a._2 > b._2) a else b
    }).map{case (c,(w,f)) => (c,w)}
    
    groups.map{ case(d,(c,f)) => (c,d)}.join(labels).map{case (c,(d,l)) => (d,l)}.collect()    
  }
  
  def filterTerm(str: String) : Boolean ={
    (str forall Character.isDigit) || str.contains(".") || str.length() == 1 || (str.head >= 'a' && str.head <= 'Z')
  }
  
  /*
  def main(args: Array[String]) {
    val docId = "1485,1840,66,471,1056,211,907,1357,338,608,763,1624,1209,9399,2031,1892,2238,2104,4341,4501,4199,1433,1297,2347,2477,2012,2607,2228,1804,2094,530,3730,4209,4587,4293,4443,3601,3726,3858,1994,340,2028,480,347,9922,645,795,365,1082,939,4024,2260,2511,2372"
    
    
    KMeans(10,5000,10)
    OutputClusterResult()
  }
  * 
  */
  
}