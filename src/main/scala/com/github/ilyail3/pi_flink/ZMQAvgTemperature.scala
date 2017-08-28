package com.github.ilyail3.pi_flink

import java.io.File

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import scala.concurrent.duration._

object ZMQAvgTemperature {
  def main(args: Array[String]): Unit = {

    // the port to connect to
    val host: String = try {
      ParameterTool.fromArgs(args).get("host")
    } catch {
      case e: Exception =>
        System.err.println("No host specified. Please run 'ZMQAvgTemperature --host <host>'")
        return
    }

    if (host == null) {
      System.err.println("No host specified. Please run 'ZMQAvgTemperature --host <host>'")
      return
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val stats = env.addSource(new ZMQStatSocket(host))

    val avgTemp = stats
      .timeWindowAll(Time.seconds(5), Time.seconds(1))
      .aggregate(new AvgTempAgg)

    avgTemp
      .addSink(new LocalFileSync(new File("/home/ilya/Desktop/TempStats"), Seq(
        1.minute,
        15.minutes,
        3.hours,
        1.day
      )))
      .setParallelism(1)

    avgTemp
      .addSink(new LCDDisplaySink(host))
      .setParallelism(1)


    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
