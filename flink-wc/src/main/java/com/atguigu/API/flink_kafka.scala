package com.atguigu.API

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object flink_kafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop201:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaDS: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties)
    )
    kafkaDS.map(
      line =>{
        val datas:Array[String]=line.split(",")
        WaterSensor(datas(0),datas(1).toLong,datas(2).toInt)
      }
    )

    kafkaDS.print()

    env.execute("sensor")


  }

  case class WaterSensor(
                          id:String, ts:Long, height:Int
                        )

}
