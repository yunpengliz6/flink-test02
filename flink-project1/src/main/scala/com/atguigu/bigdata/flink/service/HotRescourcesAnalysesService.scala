package com.atguigu.bigdata.flink.service

import java.text.SimpleDateFormat

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.bean.ApacheLog
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.HotRescourcesAnalysesDao
import com.atguigu.bigdata.flink.function.{HotResourceKeyedProcessFunction, HotResourceWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotRescourcesAnalysesService extends  TService{
  private val hotRescourcesAnalysesDao = new HotRescourcesAnalysesDao
  override def getDao(): TDao = hotRescourcesAnalysesDao

  override def analyses()={
    val dataDS: DataStream[String] = hotRescourcesAnalysesDao.readTextFile("input/apache.log")
    val logDS: DataStream[ApacheLog] = dataDS.map(

      log => {
        val datas: Array[String] = log.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        ApacheLog(
          datas(0),
          datas(1),
          sdf.parse(datas(3)).getTime,
          datas(5),
          datas(6)
        )
      }
    )
    val waterDS: DataStream[ApacheLog] = logDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLog): Long = {
          element.eventTime
        }
      }
    )
    //按照访问路径进行分组
    val logKS: KeyedStream[ApacheLog, String] = waterDS.keyBy(_.url)

    //增加窗口数据范围
    val logWS: WindowedStream[ApacheLog, String, TimeWindow] = logKS.timeWindow(Time.minutes(10),Time.seconds(5))

     //将数据进行聚合
    val aggDS: DataStream[bean.HotRescourceClick] = logWS.aggregate(
      new MyAggregateFunction[ApacheLog],
      new HotResourceWindowFunction

    )

    //根据窗口时间将数据重新分组
    val hrcKS: KeyedStream[bean.HotRescourceClick, Long] = aggDS.keyBy(_.windowEndTime)

    //将分组后的数据处理
    hrcKS.process(new HotResourceKeyedProcessFunction)


  }
}
