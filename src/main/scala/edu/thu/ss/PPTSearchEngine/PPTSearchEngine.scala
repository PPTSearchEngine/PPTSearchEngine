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

object PPTSearchEngnine {    
   val conf = new SparkConf().setAppName("app").setMaster("local[6]")
   val sc = new SparkContext(conf)  
   
   def main(args : Array[String]){ 
     val dataDir = Config.getString("dataDir") 
     val WDMdir = Config.getString("WDMdir")
     val IDMdir = Config.getString("IDMdir")
     val query = Config.getString("query")    
     val docLenDir = Config.getString("docLenDir")
     /*
      * WDM.txt, each line is word, docId, weight
      * IDM.txt, each line is word, docId, docId, docId... 
      */  
     val WDM = inputWordDocumentMatrix(WDMdir)
     val IDM = inputInverseDocumentMatrix(IDMdir)
     val docLenSq = inputDocumentLengthSquare(docLenDir)
     
     val result = search_bool(query, IDM)     
     val sortedResult = search_VSM(query, result, docLenSq, WDM)
     println("query: " + query)
     println("result: " + result.mkString(","))   
     println("sorted: " + result.mkString(","))   
     
     
     sc.stop()    
   }  
   
  
   
   def search_VSM(query:String,relatedDoc:Array[Int], docLenSq:RDD[(Int, Double)], WDM:RDD[(String, Int, Double)]):Array[Int]= {
      
      val words = query.split(" ")  
      val vec = sc.makeRDD(Array(query)).flatMap(line=>line.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b)//TODO
      val vecLenSq = vec.map({case(x,y)=>y*y}).sum()
      val wdm_rdd = WDM
      //val docLenSq = wdm_rdd.map({case(t,d,w)=>(d,w*w)}).reduceByKey((a,b)=>a+b).filter(line=>relatedDoc.contains(line._1))
      val wdm = wdm_rdd.filter(line=>words.contains(line._1) && relatedDoc.contains(line._2))  
      val cosine = wdm.map({case(t,d,w)=>(t,(d,w))}).join(vec)
                 .map({case(t,((d,w),w2))=>(d,(t,w,w2))}).join(docLenSq)
                 .map({case(d, ((t, w1, w2), docSq))=>(d,w1*w2/scala.math.sqrt(vecLenSq*docSq))}).reduceByKey((a,b)=>a+b)      
      val rt = cosine.sortBy(_._2,false).map(x=>x._1.toInt).collect()
      
      /*
      val docLenSqPrint = docLenSq.map(x=>x._1.toString+","+x._2.toString).collect().mkString("|")
      val cosinePrint = cosine.sortBy(_._2,false).map(x=>x._1.toString+","+x._2.toString).collect().mkString("|")
      println("docLen: " + docLenSqPrint)
      println("cosine: " + cosinePrint)
      * 
      */
      rt
   }
   def search_bool(inputQuery:String, IDM:RDD[(String,Array[Int])]):Array[Int] = { 
     val query = inputQuery.split(" ")
     val docTerm = IDM.filter(line=>query.contains(line._1)).flatMap(x=>x._2).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(_._2,false)
     val result = docTerm.map(x=>x._1)//TODO: tak
     result.collect()
   }

   def inputInverseDocumentMatrix(filePath:String):RDD[(String,Array[Int])]={
     val data = sc.textFile(filePath)
     data.map(line=>{
         val param = line.split(",")
         (param(0), param.tail.map(x=>x.toInt))         
     })
   }
   
   def inputWordDocumentMatrix(filePath:String):RDD[(String, Int, Double)]={
     val data = sc.textFile(filePath)
     data.map(line=>{
       val param = line.split(",")
       (param(0), param(1).toInt, param(2).toDouble)         
     })
   }
   
   def inputDocumentLengthSquare(filePath:String):RDD[(Int, Double)]={
     val data = sc.textFile(filePath)
     data.map(line=>{
       val param = line.split(",")
       (param(0).toInt, param(1).toDouble)
     })
   }

   def printWDM(WDM:Array[(String,Int, Double)]) = {
     val wStr = WDM.map({case(a,b,c) => a+","+b.toString() + ","+c.toString()}).mkString("\n")
     println(wStr)
   }
   
   def printIDM(IDM:Array[(String,Array[Int])]) = {
     val wStr = IDM.map({case(a,b) => a+","+b.mkString(",")}).mkString("\n")
     println(wStr)
   }
   
   /*
    * function : get all file in a given directory
    * output: Array, full filePath
    */
   def subdirs2(dir: File): Array[File] = {
        val d = dir.listFiles.filter(_.isDirectory)
        val f = dir.listFiles.filter(_.isFile).toIterator
        f ++ d.toIterator.flatMap(subdirs2 _)
        f.toArray
    }  
     

}