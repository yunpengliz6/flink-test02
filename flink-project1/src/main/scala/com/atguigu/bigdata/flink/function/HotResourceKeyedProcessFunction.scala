package com.atguigu.bigdata.flink.function

import java.lang
import java.sql.Timestamp

import com.atguigu.bigdata.flink.bean.HotRescourceClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class HotResourceKeyedProcessFunction extends   KeyedProcessFunction[Long, HotRescourceClick, String]{
  private var resourceList : ListState[HotRescourceClick] = _
  private var alarmTimer : ValueState[Long] = _


  override def open(parameters: Configuration): Unit = {
    resourceList = getRuntimeContext.getListState(
      new ListStateDescriptor[HotRescourceClick]("resourceList", classOf[HotRescourceClick])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
    )
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotRescourceClick, String]#OnTimerContext, out: Collector[String]): Unit = {
    val datas: lang.Iterable[HotRescourceClick] = resourceList.get()
    val list = new ListBuffer[HotRescourceClick]
    import scala.collection.JavaConversions._
    for(data<- datas){
      list.add(data)
    }
    resourceList.clear()
    alarmTimer.clear()
    val result: ListBuffer[HotRescourceClick] = list.sortWith(
      (left, right) => {
        left.clickCount > right.clickCount
      }
    ).take(3)
    val builder = new StringBuilder
    builder.append("当前时间："+ new Timestamp(timestamp)+"\n")
    for (data <- result) {
      builder.append("URL:"+data.url+",点击量："+data.clickCount + "\n")
    }
    builder.append("================")
    out.collect(builder.toString())
  }

  override def processElement(value: HotRescourceClick, ctx: KeyedProcessFunction[Long, HotRescourceClick, String]#Context, collector: Collector[String]): Unit = {
    resourceList.add(value)
    if(alarmTimer.value() ==0){
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      alarmTimer.update(value.windowEndTime)
    }
  }



}
