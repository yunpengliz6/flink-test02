package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.HotItemAnalysesDao
import com.atguigu.bigdata.flink.function.{HotItemAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotItemAnalysesService extends TService{

 private val hostItemAnalysesDao=new  HotItemAnalysesDao()
  override def getDao(): TDao = hostItemAnalysesDao
 //数据分析
  override def analyses()={
    //1 获取行为数据
    val ds: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

    //2设置时间戳水位线标记
    val timeDS: DataStream[bean.UserBehavior] = ds.assignAscendingTimestamps(_.timestamp*1000)

    //3将数据进行清洗，保存数据

    val filterDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //4将相同商品聚合到一起
    val dataKS: KeyedStream[bean.UserBehavior, Long] = filterDS.keyBy(_.itemId)

    //5设定时间窗口

    val dataWS: WindowedStream[bean.UserBehavior, Long, TimeWindow] = dataKS.timeWindow(Time.hours(1), Time.minutes(5))

    //6 聚合数据
    val hicDS: DataStream[bean.HotItemClick] = dataWS.aggregate(

      new HotItemAggregateFunction,
      new HotItemWindowFunction
    )
    // TODO 7. 将数据根据窗口重新分组
    val hicKS: KeyedStream[bean.HotItemClick, Long] = hicDS.keyBy(_.windowEndTime)

    // TODO 8. 对聚合后的数据进行排序
    val result: DataStream[String] = hicKS.process(new HotItemProcessFunction)

    result







  }


}
