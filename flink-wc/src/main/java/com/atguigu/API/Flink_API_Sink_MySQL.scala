package com.atguigu.API

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink_API_Sink_MySQL {

  def main(args: Array[String]): Unit = {

    // 转换
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 对数据流的处理可以采用Scala API，也可以采用Java API
    val dataDS: DataStream[String] = env.fromCollection(
      List("a", "b", "c")
    )

    // TODO 将数据保存到Mysql中
    dataDS.addSink( new MySQLSink() )

    env.execute()
  }
  // 自定义Sink

  class MySQLSink extends RichSinkFunction[String]{

    var conn:Connection = null
    var pstat : PreparedStatement = null

    // 建立链接
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager
        .getConnection("jdbc:mysql://linux1:3306/rdd"
          ,"root","809623"
        )
      pstat = conn.prepareStatement(
        "INSERT INTO user (id, name, age) VALUES (?, ?, ?)"
      )
    }

    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
      pstat.setInt(1, 1)
      pstat.setString(2, value)
      pstat.setInt(3, 10)
      pstat.execute()
    }

    // 释放链接
    override def close(): Unit = {
      if ( pstat != null ) {
        pstat.close()
      }
      if ( conn != null ) {
        conn.close()
      }

    }
  }

}

