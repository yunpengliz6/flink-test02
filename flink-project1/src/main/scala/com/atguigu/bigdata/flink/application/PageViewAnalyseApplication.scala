package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.PageViewAnalyseController

object PageViewAnalyseApplication extends  App with TApplication{
 start{
   val Controller = new PageViewAnalyseController
   Controller.execute()
 }
}
