package com.atguigu.bigdata.flink.common

import com.atguigu.bigdata.flink.util.FlinkStreamEnv

import scala.util.control.Breaks

trait TApplication {
  def start(op : => Unit):Unit={
          try{
            //获取flink的环境
            FlinkStreamEnv.init
            op
            //执行Flink环境

            FlinkStreamEnv.execute()
          }catch {
            case e => e.printStackTrace()
          }finally {
            FlinkStreamEnv.clear()
          }
  }
}
