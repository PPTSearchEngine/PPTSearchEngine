
package edu.thu.ss.PPTSearchEngine.WordDocumentMatrix
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.math._
import java.io.File
import collection.mutable.ArrayBuffer
import edu.thu.ss.PPTSearchEngine.Properties.Config
import java.io._
import org.apache.spark.rdd.RDD

object WordDocumentMatrix {    
  
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
     val WDM = getWordDocumentMatrix(dataDir, docLenDir)      
     outputWordDocumentMatrix(WDM,WDMdir) 
     //printWDM(WDM)   
     val IDM = getInverseDocumentMatric(WDM) 
     outputInverseDocumentMatrix(IDM, IDMdir)     
     //printIDM(IDM)    
     sc.stop()    
   }  
   
 
   def getInverseDocumentMatric(WDM:RDD[(String, Int, Double)]):RDD[(String,Array[Int])] = {
     val wordDoc = WDM.map({case(a,b,c)=>(a,Array(b))}).reduceByKey( (a,b)=>a ++: b )
     wordDoc
   }

   /*
    * function: calculate word-docuemnt matric
    * output  : Array(word, docId, weight-tfidf)
    */
   def getWordDocumentMatrix(dataDir:String, dataDir2:String):RDD[(String, Int, Double)] ={
     /*
      * val f:File = new File(dataDir)                                 
     val docPath = subdirs2(f).map(x=>x.toString)      
     val docId = docPath.map(line=>{
       val params = line.split('\\').last       
       (params.split('.'))(0).toInt  
     })   
      */
     val data = sc.textFile(dataDir)
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
     
     //output document length square
     val writer = new PrintWriter(new File(dataDir2))
     val docLenSq = dic.map({case((word,docId),freq)=>(docId,freq*freq)}).reduceByKey((a,b)=>a+b)
     val docLenSq_str = docLenSq.map({case(a,b)=>a.toString+","+b.toString}).collect().mkString("\n")
     writer.write(docLenSq_str)
     writer.close()
     
     dic_rt 
   }
     
   def outputInverseDocumentMatrix(IDM:RDD[(String,Array[Int])], outputDir:String)={
     val writer = new PrintWriter(new File(outputDir))
     val wStr = IDM.map({case(a,b) => a+","+b.mkString(",")})
     val a = wStr.collect()
     a.foreach(x=>writer.write(x+"\n"))
     writer.close()
   }
  
   def outputWordDocumentMatrix(WDM:RDD[(String, Int, Double)], outputDir:String)={
     val writer = new PrintWriter(new File(outputDir))
     val wStr = WDM.map({case(a,b,c) => a+","+b.toString() + ","+c.toString()})
     val a = wStr.collect()
     a.foreach(x=>writer.write(x+"\n"))
     writer.close()
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