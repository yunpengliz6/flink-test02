package com.atguigu.bigdata.flink.function

import java.sql.Timestamp
import java.{lang, util}

import com.atguigu.bigdata.flink.bean.HotItemClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class HotItemProcessFunction  extends  KeyedProcessFunction[Long, HotItemClick, String] {


  //数据集合
  private var itemList:ListState[HotItemClick] = _
  // 定时器
  private var alarmTimer:ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    itemList = getRuntimeContext.getListState(
      new ListStateDescriptor[HotItemClick]("itemList",classOf[HotItemClick])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]( "alarmTimer", classOf[Long] )
    )

  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, out: Collector[String]): Unit = {
  //到达定时器触发时，进行数据排序输出
    val datas: lang.Iterable[HotItemClick] = itemList.get()
    val dataIter: util.Iterator[HotItemClick] = datas.iterator()
    val list = new ListBuffer[HotItemClick]

    while (dataIter.hasNext){
      list.append(dataIter.next())
    }
    itemList.clear()
    alarmTimer.clear()
    val result: ListBuffer[HotItemClick] = list.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

    //将结果输出到控制台
    val builder =new StringBuilder
    builder.append("当前时间："+  new Timestamp(timestamp)+"\n")
    for(data<- result){
      builder.append("商品："+data.itemId+",点击数量"+data.clickCount+"\n")
    }
    builder.append("===================")
    out.collect(builder.toString())
    Thread.sleep(1000)

  }

  override def processElement(value: HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, collector: Collector[String]): Unit = {
    //将每条数据存储起来，设定定时器
    itemList.add(value)
    if(alarmTimer.value() ==0){
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      alarmTimer.update(value.windowEndTime)
    }

  }
}
