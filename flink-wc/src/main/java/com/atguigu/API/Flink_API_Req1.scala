package com.atguigu.API

import com.atguigu.API.flink_kafka.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Flink_API_Req1 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        val wsDS = sensorDS.assignAscendingTimestamps(_.ts * 1000)

        val sensorKS: KeyedStream[WaterSensor, String] =
            wsDS.keyBy(_.id)

        // TODO 监控水位传感器的水位值
        //      如果水位值在五秒钟之内连续上升，则报警。
        val processDS: DataStream[String] = sensorKS.process(
            new KeyedProcessFunction[String, WaterSensor, String] {

                // 当前水位值数据
                private var currentHeight = 0L
                // 定时器
                private var alarmTimer = 0L

                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
                    out.collect("水位传感器【"+ctx.getCurrentKey+"】"+ctx.timerService().currentWatermark()+"连续5s水位上涨" )
                }

                // 每来一条数据，方法会触发执行一次
                override def processElement(
                   value: WaterSensor, // 输入数据
                   ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, // 上下文环境
                   out: Collector[String]): Unit = { // 输出
                    if ( value.height > currentHeight ) {
                        // 如果传感器的水位值大于上一次的水位，那么准备触发定时器
                        if ( alarmTimer == 0L ) {
                            // 准备定时器
                            alarmTimer = value.ts * 1000 + 5000
                            ctx.timerService().registerEventTimeTimer(alarmTimer)
                        }
                    } else {
                        // 如果传感器的水位值小于或等于于上一次的水位，那么重置(删除)定时器
                        ctx.timerService().deleteEventTimeTimer(alarmTimer)
                        alarmTimer = value.ts * 1000 + 5000
                        ctx.timerService().registerEventTimeTimer(alarmTimer)
                    }

                    currentHeight = value.height;

                }
            }
        )
        wsDS.print("water>>>")
        processDS.print("process>>>>")

        env.execute()
    }

}
