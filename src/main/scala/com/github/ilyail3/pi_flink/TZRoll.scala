package com.github.ilyail3.pi_flink

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, TimeZone}

case class TZRoll(tzs:List[(String, TimeZone)], timeout:Long) {
  var tzIndex:Int = 0
  var tzUpdate:Long = System.currentTimeMillis()
  val df:DateFormat = new SimpleDateFormat("HH")

  var tzString:String = tzStringGen()

  private def tzStringGen():String = {
    val (tzChar, tz) = tzs(tzIndex)

    df.setTimeZone(tz)
    df.format(new Date()) + tzChar
  }

  def roll():String = {
    if(System.currentTimeMillis() - tzUpdate > timeout){
      tzIndex = (tzIndex + 1) % tzs.size

      tzUpdate = System.currentTimeMillis()
      tzString = tzStringGen()
    }

    tzString
  }
}
