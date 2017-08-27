package com.github.ilyail3.pi_flink

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Socket}

class ZMQStatSocket(host:String) extends SourceFunction[Stats] {

  @transient var context:Context = null
  @transient var socket:Socket = null
  @transient var statsRequestSocket:Socket = null
  @transient var isRunning:Boolean = false

  override def cancel(): Unit = {
    isRunning = false

    socket.close()
    statsRequestSocket.close()
    context.close()
  }

  override def run(ctx: SourceFunction.SourceContext[Stats]): Unit = {
    context = ZMQ.context(1)

    socket = context.socket(ZMQ.SUB)

    socket.connect(s"tcp://$host:5560")
    socket.subscribe("")
    socket.setReceiveTimeOut(500)

    statsRequestSocket = context.socket(ZMQ.PUSH)
    statsRequestSocket.connect(s"tcp://$host:5559")

    isRunning = true

    while(isRunning){
      //synchronized(ctx.getCheckpointLock, () => {
        val msg = socket.recv()

        if(msg != null) {
          val bb = ByteBuffer.wrap(msg)
          bb.order(ByteOrder.LITTLE_ENDIAN)

          ctx.collect(Stats(
            System.currentTimeMillis(),
            bb.getDouble(),
            bb.getDouble(),
            bb.getDouble(),
            bb.getDouble()
          ))
        } else {
          // No new data, ask for data from the server
          statsRequestSocket.send("")
        }
    }
  }

  override def toString = s"ZMQStatSocket($host)"
}
