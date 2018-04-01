package com.song.driver

import com.song.bean.{CleanConfigBean, MobileClean}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Driver {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("data_clean").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val beanList:ListBuffer[CleanConfigBean] = ListBuffer();
    beanList.append(new CleanConfigBean(sc,"/home/song/mobileinfo",MobileClean.cleanFunc))

    for(bean <- beanList){
      bean.clean()
    }

    sc.stop()
  }
}
