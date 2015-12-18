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
      
      
     val WDM = getWordDocumentMatric(dataDir)    
     outputWordDocumentMatric(WDM,WDMdir) 
     //printWDM(WDM)   
     val IDM = getInverseDocumentMatric(WDM) 
     outputInverseDocumentMatric(IDM, IDMdir)     
     //printIDM(IDM)*/
     
     /* if already have WDM.txt and IDM.txt*/
     
     val WDM = inputWordDocumentMatric(WDMdir)
     val IDM = inputInverseDocumentMatric(IDMdir)     
     val result = search_bool(query, IDM)     
     val sortedResult = search_VSM(query, result, WDM)
     println("query: " + query)
     println("result: " + result.mkString(","))   
     println("sorted: " + result.mkString(","))   
     sc.stop()    
   }  
   
   def queryToArray(query:String) : Array[(String,Double)]={
     val temp = Array((query))
     val result = sc.makeRDD(temp).flatMap(line=>line.split(" ")).map(x=>(x,1.0)).reduceByKey((a,b)=>a+b)
     result.collect()
   }
   
   def search_VSM(query:String,relatedDoc:Array[Int], WDM:Array[(String, Int, Double)]):Array[Int]= {
      
      val words = query.split(" ")  
      val vec = sc.makeRDD(queryToArray(query))
      val vecLenSq = vec.map({case(x,y)=>y*y}).sum()
      val wdm_rdd = sc.makeRDD(WDM)
      val docLenSq = wdm_rdd.map({case(t,d,w)=>(d,w*w)}).reduceByKey((a,b)=>a+b).filter(line=>relatedDoc.contains(line._1))
      val wdm = wdm_rdd.filter(line=>words.contains(line._1) && relatedDoc.contains(line._2))  
      val cosine = wdm.map({case(t,d,w)=>(t,(d,w))}).join(vec)
                 .map({case(t,((d,w),w2))=>(d,(t,w,w2))}).join(docLenSq)
                 .map({case(d, ((t, w1, w2), docSq))=>(d,w1*w2/scala.math.sqrt(vecLenSq*docSq))}).reduceByKey((a,b)=>a+b)      
      val rt = cosine.sortBy(_._2,false).map(x=>x._1.toInt).collect()
      
      
      val docLenSqPrint = docLenSq.map(x=>x._1.toString+","+x._2.toString).collect().mkString("|")
      val cosinePrint = cosine.sortBy(_._2,false).map(x=>x._1.toString+","+x._2.toString).collect().mkString("|")
      println("docLen: " + docLenSqPrint)
      println("cosine: " + cosinePrint)
      rt
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
     val wordDoc = sc.parallelize(WDM).map({case(a,b,c)=>(a,Array(b))}).reduceByKey( (a,b)=>a ++: b )
     val rt = wordDoc.collect()
		 rt
   }

   /*
    * function: calculate word-docuemnt matric
    * output  : Array(word, docId, weight)
    */
   def getWordDocumentMatric(dataDir:String):Array[(String, Int, Double)] ={
     /*
      * val f:File = new File(dataDir)                                 
     val docPath = subdirs2(f).map(x=>x.toString)      
     val docId = docPath.map(line=>{
       val params = line.split('\\').last       
       (params.split('.'))(0).toInt  
     })   
      */
     val data = sc.textFile(dataDir+"//total_5000.txt")
     val dic = data.flatMap(line=>{
       val param = line.split('|')
       //line : docID | title | class | url | time | content
       if(param.length >= 6){
         val docID = param(0).toInt
         val words = param(5).split(" ")
         words.zipAll(Array(docID), "NULL",docID)
       }
       else
         Array(("NULL",0))
     }).map({case(x,y)=>((x,y),1.0)})//.reduceByKey((a,b)=>a+b).map({case((a,b),c)=>(a,b,c)})   
     //dic.collect()
     
     val docLen = dic.map({case((word,docId),freq)=>(docId,freq)}).reduceByKey((a,b)=>a+b)
     //(word, wordFreq) how many times a word appears in all documents
     val wordNum = dic.map({case((word, docId),freq)=>(word, 1.0)}).reduceByKey((a,b)=>a+b)
     val dic_tfidf = dic.map({case((word,doc),freq)=>(word,(doc,freq))}).join(wordNum)
                         .map({case(word,((doc,freq),wordNum)) => (doc, (word, freq, wordNum))}).join(docLen)
                         .map({case(doc, ((word, freq, wordNum),docLen)) => (word, doc, freq/docLen * scala.math.log(wordNum/(freq.toDouble+1) + 1))})
     
     val dic_rt = dic_tfidf.distinct     
     
     dic_rt.collect()   
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
     val data = sc.textFile(filePath)
     val rt = data.map(line=>{
         if(line != "")
         {
           //word, docId, docId, docId...
           val param = line.split(",")
           (param(0), param.tail.map(x=>x.toInt))
         }
         else
           ("Null",Array[Int](0))
     })
     rt.collect()
   }
   
   def inputWordDocumentMatric(filePath:String):Array[(String, Int, Double)]={
     val data = sc.textFile(filePath)
     val rt = data.map(line=>{
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
     rt.collect()
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