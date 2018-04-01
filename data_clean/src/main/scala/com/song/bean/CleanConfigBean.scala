package com.song.bean

import org.apache.spark.SparkContext

class CleanConfigBean(sc:SparkContext,inputPath:String,cleanFunc: (SparkContext,String)=>Unit) {

  def clean(){
    cleanFunc(sc,inputPath)
  }
}
