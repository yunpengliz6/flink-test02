package com.atguigu.bigdata.flink.function

import com.atguigu.bigdata.flink.bean.HotRescourceClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class HotResourceWindowFunction extends WindowFunction[ Long, HotRescourceClick, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[HotRescourceClick]): Unit = {

    out.collect( HotRescourceClick( key, input.iterator.next(), window.getEnd ) )
  }

}
