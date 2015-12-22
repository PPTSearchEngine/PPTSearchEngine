package edu.thu.ss.PPTSearchEngine

import edu.thu.ss.PPTSearchEngine.Properties.Config
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object App {

  def main(args: Array[String]) {
    //val t1 = new test
    //val t2 = new test
    //println(t1.a)
    //print
    /* val conf = new SparkConf().setAppName("app").setMaster("local[5]")
     conf.set("spark.driver.allowMultipleContexts" , "true") 
    val sc = new SparkContext(conf)
   */ 
    val searcher =  PPTSearcher

    val result = searcher.search("吴彦祖")
    var resStr = ""
    var i = 0
    while(i < result.RetrievalDoc.length){
      resStr += result.RetrievalDoc(i).DocId + "|" + result.RetrievalDoc(i).Title + "|" +result.RetrievalDoc(i).Url + "|" +   result.RetrievalDoc(i).Date + "|" + result.RetrievalDoc(i).label + "|" +result.RetrievalDoc(i).Abstract + "||\n" 
    i+= 1
    }
     println(resStr)
    
    /*
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val docIds = Array(3504)
    val data = sc.textFile(Config.getString("dataDir"))
     .filter { line => docIds.contains(line.split('|').head.toInt) }
      .map { line => line.split('|') }.collect()
      println(data.size)
      * 
      */
  }

}

