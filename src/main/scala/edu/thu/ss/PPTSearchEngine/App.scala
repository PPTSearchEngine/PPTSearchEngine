package edu.thu.ss.PPTSearchEngine

import edu.thu.ss.PPTSearchEngine.Properties.Config

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println(Config.getString("test"))
  }

}
