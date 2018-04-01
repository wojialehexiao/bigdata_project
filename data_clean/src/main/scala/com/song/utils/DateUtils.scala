package com.song.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtils {

  def getYesterday(): String = {
    val dt: Date = new Date()
    val yesterday = getDaysBefore(dt, 1)
    return yesterday
  }

  def getDaysBefore(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);
    cal.add(Calendar.DATE, - interval)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
}
