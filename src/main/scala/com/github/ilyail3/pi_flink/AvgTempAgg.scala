package com.github.ilyail3.pi_flink

import org.apache.flink.api.common.functions.AggregateFunction



class AvgTempAgg extends AggregateFunction[Stats, AvgTempAcc, AvgPressureTemp] {
  override def add(value: Stats, accumulator: AvgTempAcc):Unit = {
    accumulator.sum += value.temp
    accumulator.pressure += value.pressure

    accumulator.number += 1
    accumulator.minTime = Math.min(accumulator.minTime, value.time)
  }

  override def createAccumulator() = AvgTempAcc(0, 0, 0, Long.MaxValue)

  override def getResult(accumulator: AvgTempAcc):AvgPressureTemp =
    if(accumulator.number == 0) AvgPressureTemp(0, 0, 0, 0)
    else AvgPressureTemp(
      System.currentTimeMillis() - accumulator.minTime,
      accumulator.sum / accumulator.number.toDouble,
      accumulator.pressure / accumulator.number.toDouble,
      accumulator.number
    )

  override def merge(a: AvgTempAcc, b: AvgTempAcc):AvgTempAcc =
    AvgTempAcc(
      sum = a.sum + b.sum,
      pressure = a.pressure + b.pressure,
      number = a.number + b.number,
      minTime = Math.min(a.minTime, b.minTime)
    )


  override def toString = s"AvgTempAgg"
}
