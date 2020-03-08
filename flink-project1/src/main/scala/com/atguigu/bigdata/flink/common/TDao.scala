package com.atguigu.bigdata.flink.common

import com.atguigu.bigdata.flink.util.FlinkStreamEnv

//通用数据访问特质
trait TDao {
  //读取文件
    def readTextFile( implicit path :String)={
      FlinkStreamEnv.get().readTextFile(path)

     }
     def readKafka():Unit={

     }
     def readSocket():Unit={

    }


}
