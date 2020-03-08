package com.atguigu.API

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.util.Random

object Flink_API_Transform_map {

  def main(args: Array[String]): Unit = {

    // 转换
    // TODO Transform - map
    val env: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // TODO Transform - flatMap
    val listDS: DataStream[Int] = env.fromCollection(
      List(
        1,2,3,4
      )
    )

    val flatDS: DataStream[Int] = listDS.flatMap(num=>List(num))

    flatDS.print("flatMap>>>")

    env.execute()
  }
}
