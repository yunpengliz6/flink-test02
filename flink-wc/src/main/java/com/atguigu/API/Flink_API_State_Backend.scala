package com.atguigu.API

import com.atguigu.API.flink_kafka.WaterSensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object Flink_API_State_Backend {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //设定状态后端
    //默认的状态后端为内存
    val cappth ="hdfs://hadoop:9000/test"
    val backend:StateBackend = new RocksDBStateBackend(cappth)
    env.setStateBackend(backend)


    //启动检查点
    env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)
    val dataDS: DataStream[String] = env.socketTextStream("localhost",9999)
    val sensorDS: DataStream[WaterSensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )
    val wsDS: DataStream[WaterSensor] = sensorDS.assignAscendingTimestamps(_.ts*1000)

    val sensorKS: KeyedStream[WaterSensor, String] = wsDS.keyBy(_.id)

    val processDS: DataStream[String] = sensorKS.process(
      new KeyedProcessFunction[String, WaterSensor, String] {

        private var currentHeight: ValueState[Long] = _
        private var alarmTimer: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          currentHeight = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("currentHeight", classOf[Long])
          )
          alarmTimer = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
          )
        }


        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("水位传感器【" + ctx.getCurrentKey + "】" + ctx.timerService().currentWatermark() + "连续5s水位上涨")
        }


        override def processElement(
                                     value: WaterSensor, // 输入数据
                                     ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, // 上下文环境
                                     out: Collector[String]): Unit = { // 输出

          // 使用有状态类型的变量
          // 获取变量的值： stateVar.value()
          // 更新变量的值： stateVar.update()
          // 清除变量的值： stateVar.clear()
          if (value.height > currentHeight.value()) {
            if (alarmTimer.value() == 0L) {
              alarmTimer.update(value.ts * 1000 + 5000)
              ctx.timerService().registerEventTimeTimer(alarmTimer.value())
            }
          } else {
            ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
            alarmTimer.update(value.ts * 1000 + 5000)
            ctx.timerService().registerEventTimeTimer(alarmTimer.value())
          }

          currentHeight.update(value.height);

        }
      }
    )
    processDS
    wsDS.print("water>>>")
    processDS.print("process>>>>")

    env.execute()
  }

}
