package com.vdian.learnflink.jiangfuqiang

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author: jiangfuqiang
  * date:2019/12/17
  *
  */
object WindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputData:DataStream[String] = env.socketTextStream("localhost", 9090)

    val convertedData: DataStream[(String, Int)] = inputData.filter(_.nonEmpty).map(new MapFunction[String, (String, Int)] {
      override def map(t: String): (String, Int) = {
        val values = t.split(" ")
        (values(0), values(1).toInt)
      }
    })
    //每2秒统计5秒内的数据
    val resultData = convertedData.keyBy(_._1).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
          (t._1, t._2 + t1._2)
        }
      })
    resultData.print()

    env.execute("Tumbling Window test")

  }

}
