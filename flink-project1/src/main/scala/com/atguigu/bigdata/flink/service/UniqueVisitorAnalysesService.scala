package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.UniqueVisitorAnalysesDao
import com.atguigu.bigdata.flink.function.{UniqueVisitorAnalusesByBloomFilterWindowFunction, UniqueVisitorAnalysesWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class UniqueVisitorAnalysesService extends TService {
  private val uniqueVisitorAnalysesDao = new UniqueVisitorAnalysesDao

  override def getDao(): TDao = uniqueVisitorAnalysesDao

  def uvAnalyses() = {
    //获取行为数据
    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()
    //抽取时间戳水位标记
    val waterDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)
    //将数据结构转换
    val userDS: DataStream[(Long, Int)] = waterDS.map(
      data => {
        (data.userId, 1)
      }
    )
    //将所有的数据放入一个窗口中
    val dataWS: AllWindowedStream[(Long, Int), TimeWindow] = userDS.timeWindowAll(Time.hours(1))

    //判断一个小时窗口中不重复的用户ID的个数
    dataWS.process(new UniqueVisitorAnalysesWindowFunction)
  }

  def uvAnalysesByBloomFilter() = {
    // 获取用户行为数据
    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

    // 抽取时间戳和水位线标记
    val waterDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

    //  将数据进行结构的转换
    val userDS: DataStream[(Long, Int)] = waterDS.map(
      data => {
        (data.userId, 1)
      }
    )

    // 设定窗口范围
    // 将所有的数据放入一个窗口中
    val dataWS: AllWindowedStream[(Long, Int), TimeWindow] = userDS.timeWindowAll(Time.hours(1))


    dataWS.trigger(
      new Trigger[(Long, Int), TimeWindow]() {
        override def onElement(t: (Long, Int), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.FIRE_AND_PURGE
        }

        override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

        }
      }
    ).process(
      new UniqueVisitorAnalusesByBloomFilterWindowFunction
    )
  }

  override def analyses() = {

    // UV统计（布隆过滤器）
    uvAnalysesByBloomFilter

  }
}

