package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.HotItemAnalysesController

object HotItemAnalysesApplication extends App with TApplication{


  //启动应用程序
  start{

    val controller = new HotItemAnalysesController
    controller.execute()

  }
}
