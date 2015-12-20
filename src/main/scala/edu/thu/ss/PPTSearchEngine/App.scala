package edu.thu.ss.PPTSearchEngine

import edu.thu.ss.PPTSearchEngine.Properties.Config
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object App {

  def main(args: Array[String]) {
    
    val searcher = new PPTSearcher
    val result = searcher.search(Config.getString("query"))
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

