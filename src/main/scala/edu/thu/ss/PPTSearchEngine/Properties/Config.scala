package edu.thu.ss.PPTSearchEngine.Properties

import java.util.MissingResourceException;
import java.util.ResourceBundle;

object Config {
  val BUNDLE_NAME = "edu.thu.ss.PPTSearchEngine.Properties.config"
  
  val RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME) 
  
  def getString(key: String) : String = {
    RESOURCE_BUNDLE.getString(key)
  }
}