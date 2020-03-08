package com.atguigu.API


import com.atguigu.API.flink_kafka.WaterSensor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Flink_API_Process_Keyed {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        val sensorKS: KeyedStream[WaterSensor, String] =
            sensorDS.keyBy(_.id)

        val processDS: DataStream[String] = sensorKS.process(
            new KeyedProcessFunction[String, WaterSensor, String] {

                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
                    // 定时器在指定的时间点触发该方法的执行
                    out.collect("timer execute...")
                }

                // 每来一条数据，方法会触发执行一次
                override def processElement(
                                                   value: WaterSensor, // 输入数据
                                                   ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, // 上下文环境
                                                   out: Collector[String]): Unit = { // 输出

                    out.collect("keyedProcess = " + ctx.timestamp())

                    // 注册定时器
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())
                }
            }
        )
        processDS.print("process>>>>")

        env.execute()
    }

}
