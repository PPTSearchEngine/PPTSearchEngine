package edu.thu.ss.PPTSearchEngine.wordDocument
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.math._
import java.io.File
import collection.mutable.ArrayBuffer
import edu.thu.ss.PPTSearchEngine.Properties.Config
import java.io._

object wordDocument {    
   val conf = new SparkConf().setAppName("app").setMaster("local[4]")
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
     
     val WDM = getWordDocumentMatric(dataDir)    
     outputWordDocumentMatric(WDM,WDMdir) 
     //printWDM(WDM)   
     val IDM = getInverseDocumentMatric(WDM) 
     outputInverseDocumentMatric(IDM, IDMdir)     
     //printIDM(IDM)
     
     /* if already have WDM.txt and IDM.txt*/
     /*
     val WDM = inputWordDocumentMatric(WDMdir)
     val IDM = inputInverseDocumentMatric(IDMdir)
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
   }
   
   def getInverseDocumentMatric(WDM:Array[(String, Int, Double)]):Array[(String,Array[Int])] = {
     val WDM_ = WDM.map({case(a,b,c)=>(a,Array(b))})
     val wordDoc = sc.parallelize(WDM_).reduceByKey( (a,b)=>a ++: b )
     val rt = wordDoc.collect()
		 rt
   }

   /*
    * function: calculate word-docuemnt matric
    * output  : Array(word, docId, weight)
    */
   def getWordDocumentMatric(dataDir:String):Array[(String, Int, Double)] = {
     val f:File = new File(dataDir)                                 
     val docPath = subdirs2(f).map(x=>x.toString)      
     val docId = docPath.map(line=>{
       val params = line.split('\\').last       
       (params.split('.'))(0).toInt  
     })
     
     var dictionary = Array[((String, Int), Double)]()
     for (i <- 0 to docId.length-1){
       val words = getWords(docPath(i), docId(i))//((word, docId), freq)       
       dictionary ++= words
     }
     val dic_s = sc.parallelize(dictionary.toList)
     val dic_wordFreq = dic_s.reduceByKey((a,b)=>a+b).map({case((a,b),c)=>(a,b,c)}) 
     
     //tf*idf
     //(docId, docLen) how many word a document contains
     val docLen = dic_s.map({case((word,docId),freq)=>(docId,freq.toDouble)}).reduceByKey((a,b)=>a+b)
     //(word, wordFreq) how many times a word appears in all documents
     val wordNum = dic_s.map({case((word, docId),freq)=>(word, 1.0)}).reduceByKey((a,b)=>a+b)
     val dic_tfidf = dic_s.map({case((word,doc),freq)=>(word,(doc,freq))}).join(wordNum)
                         .map({case(word,((doc,freq),wordNum)) => (doc, (word, freq, wordNum))}).join(docLen)
                         .map({case(doc, ((word, freq, wordNum),docLen)) => (word, doc, freq.toDouble/docLen * scala.math.log(wordNum/(freq.toDouble+1) + 1))})
     
     val dic_rt = dic_tfidf.distinct   
     
     //val dic_rt = dic_wordFreq
     dic_rt.collect()
   } 
   /*
    * function : get words from file
    * output   : Array, element ((word, docId), 1)
    */
   def getWords(docPath :String, docId:Int) : Array[((String, Int), Double)]= {        
      val line : String = Source.fromFile(docPath).mkString
      if(line != "" && line.split('|').length > 4)
      {
        //line : title | class | url | time | content
        val words = (line.split('|'))(4).split(" ")
        val rt = words.map( line=>((line,docId),1.0))
        rt
      }
      else
      {
        Array((("NULL",0),0.0))  
      }
   } 
   
   def outputInverseDocumentMatric(IDM:Array[(String,Array[Int])], outputDir:String)={
     val writer = new PrintWriter(new File(outputDir))
     val wArr = IDM.map({case(a,b) => a+","+b.mkString(",")})
     val wStr = wArr.mkString("\n")
     writer.write(wStr)
     writer.close()
   }
  
   def outputWordDocumentMatric(WDM:Array[(String, Int, Double)], outputDir:String)={
     val writer = new PrintWriter(new File(outputDir))
     val wArr = WDM.map({case(a,b,c) => a+","+b.toString() + ","+c.toString()})
     val wStr = wArr.mkString("\n")
     writer.write(wStr)
     writer.close()
   }
   
   def inputInverseDocumentMatric(filePath:String):Array[(String,Array[Int])]={
     val data = Source.fromFile(filePath).mkString
     data.split('\n').map(line=>{
         if(line != "")
         {
           //word, docId, docId, docId...
           val param = line.split(",")
           (param(0), param.tail.map(x=>x.toInt))
         }
         else
           ("Null",Array[Int](0))
     })
   }
   
   def inputWordDocumentMatric(filePath:String):Array[(String, Int, Double)]={
     val data = Source.fromFile(filePath).mkString
     data.split('\n').map(line=>{
       if(line != "")
       {
         val param = line.split(",")
         if(param(1) != "" && param(2) != "")
           (param(0), param(1).toInt, param(2).toDouble)
         else
           ("NULL", 0, 0.0)
       }
       else
         ("NULL", 0, 0.0)       
     })
   }

   def printWDM(WDM:Array[(String,Int, Double)]) = {
     val wArr = WDM.map({case(a,b,c) => a+","+b.toString() + ","+c.toString()})
     val wStr = wArr.mkString("\n")
     println(wStr)
   }
   
   def printIDM(IDM:Array[(String,Array[Int])]) = {
     val wArr = IDM.map({case(a,b) => a+","+b.mkString(",")})
     val wStr = wArr.mkString("\n")
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