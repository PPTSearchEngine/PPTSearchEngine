package edu.thu.ss.PPTSearchEngine.wordDocument
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import collection.mutable.ArrayBuffer
import edu.thu.ss.PPTSearchEngine.Properties.Config
import java.io._

object wordDocument {    
   val conf = new SparkConf().setAppName("app").setMaster("local")
   val sc = new SparkContext(conf)  
  
   def main(args : Array[String]){    
         
     val dataDir = Config.getString("dataDir") 
     val WDMdir = Config.getString("WDMdir")
     val IDMdir = Config.getString("IDMdir")
     val query = Config.getString("query")    
       
     /*
      * WDM.txt, each line is word, docId, weight
      * IDM.txt, each line is word, docId, docId, docId...
      */
     //if do not have WDM.txt
     //val WDM = getWordDocumentMatric(dataDir)    
     //outputWordDocumentMatric(WDM,WDMdir) 
     
     //already have WDM.txt
     val WDM = inputWordDocumentMatric(WDMdir)
     //inverse document matric 
     val IDM = getInverseDocumentMatric(WDM) 
     outputInverseDocumentMatric(IDM, IDMdir)     
     
     /*
      * search_bool : bool model, intersect, return document containing all term
      */
     val result = search_bool(query, IDM)     
     println("query: " + query)
     println("result: " + result.mkString(","))
     
     
     sc.stop()    
   }   
   
   def search_bool(inputQuery:String, IDM:Array[(String,Array[Int])]):Array[Int] = {          
     val query = inputQuery.split(" ")   
     val IDM_ = IDM.toMap
     var queryResult = Set[Int]()
     var init = 1
     query.foreach(q =>{
       val idx = if(IDM_.contains(q)) IDM_(q) else Array(0)
       if(idx != Array(0)){
         if(init == 1)
         {
           queryResult = idx.toSet
           init = 0
         }
         else{
           queryResult = queryResult.&(idx.toSet)
         }
       }
     })
     queryResult.toArray
     /*
     val result = sc.parallelize(queryResult).map(x=>(x, 1)).reduceByKey((a,b)=>a+b).sortBy(_._2,false)
     val rt = result.collect()
     //print query result
     print ("query result:")
     val docArr = rt.map({case(a,b)=>a.toString()+"," +b.toString()})
     val docStr = docArr.mkString("|")
     println(docStr)
     * 
     */
   }
      
   def getInverseDocumentMatric(WDM:Array[(String, Int, Int)]):Array[(String,Array[Int])] = {
     val WDM_ = WDM.map({case(a,b,c)=>(a,Array(b))})
     val wordDoc = sc.parallelize(WDM_).reduceByKey( (a,b)=>a ++: b )
     val rt = wordDoc.collect()
     //print rt
     /*
     rt.foreach( word => {
       val docArr = (word._2).sortBy(r=>(r._2, r._1))
       val docArraySort = docArr.map({case(a,b)=>a.toString()+"," +b.toString()})
       val docStr = docArr.mkString("|")
       println(word._1+":" + docStr)
       }
     )*/
		 rt
   }
   def outputInverseDocumentMatric(IDM:Array[(String,Array[Int])], outputDir:String)={
     val writer = new PrintWriter(new File(outputDir))
     val wArr = IDM.map({case(a,b) => a+","+b.mkString(",")})
     val wStr = wArr.mkString("\n")
     writer.write(wStr)
     writer.close()
   }

   /*
    * function: calculate word-docuemnt matric
    * output  : Array(word, docId, weight)
    */
   def getWordDocumentMatric(dataDir:String):Array[(String, Int, Int)] = {
     val f:File = new File(dataDir)                                 
     val docPath = subdirs2(f).map(x=>x.toString)      
     val docId = docPath.map(line=>{
       val params = line.split('\\').last       
       (params.split('.'))(0).toInt  
     })
     
     var dictionary = Array[((String, Int), Int)]()
     for (i <- 0 to docId.length-1){
       val words = getWords(docPath(i), docId(i))
       dictionary ++= words
     }
     val dic_s = sc.parallelize(dictionary.toList)
     val rt = dic_s.reduceByKey((a,b)=>a+b).map({case((a,b),c)=>(a,b,c)})        
     rt.collect()
   } 
   
   def outputWordDocumentMatric(WDM:Array[(String, Int, Int)], outputDir:String)={
     val writer = new PrintWriter(new File(outputDir))
     val wArr = WDM.map({case(a,b,c) => a+","+b.toString() + ","+c.toString()})
     val wStr = wArr.mkString("\n")
     writer.write(wStr)
     writer.close()
   }
   
   def inputWordDocumentMatric(filePath:String):Array[(String, Int, Int)]={
     val data = Source.fromFile(filePath).mkString
     val data1 = data.split('\n').map(line=>{
       if(line != "")
       {
         val param = line.split(",")
         if(param(1) != "" && param(2) != "")
           (param(0), param(1).toInt, param(2).toInt)
         else
           ("NULL", 0, 0)
       }
       else
         ("NULL", 0, 0)       
     })
     data1
   }
   /*
    * function : get words from file
    * output   : Array, element ((word, docId), 1)
    */
   def getWords(docPath :String, docId:Int) : Array[((String, Int), Int)]= {        
      val line : String = Source.fromFile(docPath).mkString
      if(line != "" && line.split('|').length > 4)
      {
        //line : title | class | url | time | content
        val words = (line.split('|'))(4).split(" ")
        val rt = words.map( line=>((line,docId),1))
        rt
      }
      else
      {
        Array((("NULL",0),0))  
      }
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