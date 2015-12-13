package edu.thu.ss.PPTSearchEngine

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import collection.mutable.ArrayBuffer
import edu.thu.ss.PPTSearchEngine.Properties.Config

object App {  
  
   val conf = new SparkConf().setAppName("app").setMaster("local")
   val sc = new SparkContext(conf)  
  
   def main(args : Array[String]){
     val query = Config.getString("query")     
     val dataDir = Config.getString("dataDir")
     println(query)
     println(dataDir)
     search(dataDir, query)
     sc.stop()    
   }   
   
   def search(dataDir:String,rawQuery:String)={          
     //val dataDir = "F://scala-SDK-4.2.0-vfinal-2.11-win32.win32.x86_64//eclipse//workspace//searchEngine//data"   
     val f:File = new File(dataDir)                                 
     val docPaths = subdirs2(f).map(x=>x.toString)      
     val docIds = docPaths.map(line=>{
       val params = line.split('\\').last       
       (params.split('.'))(0).toInt  
     })
     
     val query = rawQuery.split(" ")
     val WDM = getWordDocumentMatric(docPaths, docIds)
     val IDM = getInverseDocumentMatric(WDM).toMap
     var queryResult = Array[(Int, Int)]()
     query.foreach(q =>{
       val idx = if(IDM.contains(q)) IDM(q) else Array((0,0))
       if(idx != Array((0,0)))
         queryResult ++= idx    
       })
     val result = sc.parallelize(queryResult)
                    .map({case(docId,freq)=>(docId, 1)})
                    .reduceByKey((a,b)=>a+b).sortBy(_._2,false)
     val rt = result.collect()
     //print query result
     print ("query result:")
     val docArr = rt.map({case(a,b)=>a.toString()+"," +b.toString()})
     val docStr = docArr.mkString("|")
     println(docStr)
   }

   def getWordDocumentMatric(docPath:Array[String], docId:Array[Int]):Array[(String,Array[(Int,Int)])] = {
     //init dictionary
     val temp = Array((0,0))
     var dictionary = Array(("NULL",temp))    
     for (i <- 0 to docId.length-1){
       val words = getWords(docPath(i), docId(i))
       dictionary ++= words
     }
     dictionary
   }
     
   def getInverseDocumentMatric(dictionary:Array[(String,Array[(Int,Int)])]):Array[(String,Array[(Int,Int)])] = {
     val wordDoc = sc.parallelize(dictionary).reduceByKey( (a,b)=>a ++: b )
     val rt = wordDoc.collect()
     //print rt
     rt.foreach( word => {
       val docArr = (word._2).sortBy(r=>(r._2, r._1))
       val docArraySort = docArr.map({case(a,b)=>a.toString()+"," +b.toString()})
       val docStr = docArr.mkString("|")
       println(word._1+":" + docStr)
       }
     )
     rt
   }
   
   //return : Array, (word, (docId, term))
   def getWords(docPath :String, docId:Int) : Array[(String,Array[(Int,Int)])]= {        
      val line : String = Source.fromFile(docPath).mkString
      if(line != "" && line.split('|').length > 4)
      {
        val words = (line.split('|'))(4).split(" ").toList
        //title | class | url | time | content
        val rt = sc.parallelize(words)
        val r2 = rt.map(line=>((line,docId),1))
                   .reduceByKey((a,b)=>a+b)
                   .map({case((a,b),c)=>(a,Array((b,c)))})
        r2.collect() 
      }
      else
      {
        val temp = Array((0,0))
        Array(("NULL",temp))  
      }
   }
   
   def subdirs2(dir: File): Array[File] = {
        val d = dir.listFiles.filter(_.isDirectory)
        val f = dir.listFiles.filter(_.isFile).toIterator
        f ++ d.toIterator.flatMap(subdirs2 _)
        f.toArray
    }

}

