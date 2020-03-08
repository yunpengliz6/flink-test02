package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.HotItemAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class HotItemAnalysesController extends TController{

private val hotItemAnalysesService = new  HotItemAnalysesService

  override def execute(): Unit = {
    val result = hotItemAnalysesService.analyses()
    result.print()

  }
}
