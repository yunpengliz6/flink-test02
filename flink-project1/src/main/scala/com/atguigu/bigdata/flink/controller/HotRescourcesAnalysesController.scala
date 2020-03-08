package com.atguigu.bigdata.flink.controller


import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.HotRescourcesAnalysesService


class HotRescourcesAnalysesController extends TController{
  private val hotRescourcesAnalysesService = new HotRescourcesAnalysesService
  override def execute(): Unit = {
    val result = hotRescourcesAnalysesService.analyses()
    result.print()

  }
}
