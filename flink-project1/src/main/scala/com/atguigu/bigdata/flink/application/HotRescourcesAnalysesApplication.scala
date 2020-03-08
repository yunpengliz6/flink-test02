package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.HotRescourcesAnalysesController

object HotRescourcesAnalysesApplication extends App with TApplication {
  start {
  val controller = new HotRescourcesAnalysesController
    controller.execute()
  }
}
