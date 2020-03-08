package com.atguigu.bigdata.flink.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object FlinkStreamEnv {

         private val envLocal =new ThreadLocal[StreamExecutionEnvironment]
  //初始化
  def init()={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    envLocal.set(env)
    env
  }

  //返回Env
  def get() ={

     var env= envLocal.get()
    if(env == null){
         env= init()
    }
    env

  }
  def clear()={
    envLocal.remove()
  }
  def execute():Unit ={
    get().execute("application")

  }
}
