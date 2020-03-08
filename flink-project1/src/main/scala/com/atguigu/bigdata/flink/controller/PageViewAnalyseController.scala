package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.dao.PageViewAnalyseDao
import com.atguigu.bigdata.flink.service.PageViewAnalyseService

class PageViewAnalyseController extends TController{
  val pageViewAnalyseService = new   PageViewAnalyseService
  override def execute(): Unit = {

       val result= pageViewAnalyseService.analyses()
      result.print()
  }
}
