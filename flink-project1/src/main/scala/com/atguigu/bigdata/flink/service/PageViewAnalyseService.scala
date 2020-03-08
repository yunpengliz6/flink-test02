package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.PageViewAnalyseDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class PageViewAnalyseService extends TService{
  private val pageViewAnalyseDao = new PageViewAnalyseDao
  override def getDao(): TDao = pageViewAnalyseDao

  override def analyses()={
    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

   val  pvToSumDS =dataDS.assignAscendingTimestamps(_.timestamp*1000).filter(_.behavior=="pv")
      .map(pv=>("pv",1)).keyBy(_._1).timeWindow(Time.hours(1)).sum(1)
    pvToSumDS
  }
}
