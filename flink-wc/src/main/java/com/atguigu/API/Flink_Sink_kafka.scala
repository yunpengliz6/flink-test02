package com.atguigu.bigdata.flink


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Flink_Sink_kafka {

  def main(args: Array[String]): Unit = {

    // 转换
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 对数据流的处理可以采用Scala API，也可以采用Java API
    val dataDS: DataStream[String] = env.fromCollection(
      List("a", "b", "c")
    )

    val topic = "waterSensor1"
    val kafkaSink = dataDS.addSink(new FlinkKafkaProducer011[String](
      "hadoop201:9092",
      "sensor",
      new SimpleStringSchema()
    ))

    env.execute()
  }

}