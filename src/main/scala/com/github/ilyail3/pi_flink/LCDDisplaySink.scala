package com.github.ilyail3.pi_flink

import java.util.TimeZone

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Socket}

class LCDDisplaySink(address: String) extends RichSinkFunction[AvgPressureTemp] {
  @transient var context: Context = null
  @transient var socket: Socket = null
  @transient var tzRoll: TZRoll = null

  override def invoke(value: AvgPressureTemp) = {
    socket.send("Temp:%.1fC %s\nPres:%.2f hPa".format(
      value.averageTemp,
      tzRoll.roll(),
      value.averagePressure / 100.0
    ))
  }

  override def close() = {
    socket.close()
    context.close()
  }

  override def open(parameters: Configuration) = {
    context = ZMQ.context(1)
    socket = context.socket(ZMQ.PUSH)
    socket.connect(s"tcp://$address:5558")

    tzRoll = TZRoll(List(
      "L" -> TimeZone.getDefault,
      "U" -> TimeZone.getTimeZone("UTC"),
      "I" -> TimeZone.getTimeZone("Asia/Jerusalem")
    ), 1000 * 3)
  }

  override def toString = s"LCDDisplaySink($address)"
}
