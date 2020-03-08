package com.atguigu.API

import java.text.SimpleDateFormat

import com.atguigu.API.flink_kafka.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink_API_Time_Watermark1 {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // Watermark : 水位线（水印）
        val dataDS: DataStream[String] = env.socketTextStream("localhost",9999)
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        val markDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 抽取事件时间,以毫秒为单位
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
        )
        val applyDS: DataStream[String] = markDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .apply(
                    (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        val start = window.getStart
                        val end = window.getEnd
                        out.collect(s"[${start}-${end}), 数据[${datas}]")
                    }
                )


        markDS.print("mark>>>")
        applyDS.print("window>>>")


        env.execute()
    }

}
