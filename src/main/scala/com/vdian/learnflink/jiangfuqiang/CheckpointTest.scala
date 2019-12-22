package com.vdian.learnflink.jiangfuqiang

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * author: jiangfuqiang
  * date:2019/12/17
  *
  */
object CheckpointTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getCheckpointConfig.setCheckpointInterval(2000L)
    env.setStateBackend(new FsStateBackend("file:///Users/jiang/logs/flinkstate"))

    val inputData: DataStream[String] = env.socketTextStream("localhost", 9090)

    val convertedData: DataStream[(String, String, Int)] = inputData.filter(_.nonEmpty)
      .map(new MapFunction[String, (String, String, Int)]{
        override def map(t: String): (String, String, Int) = {
          val values = t.split(" ")
          (values(0), values(1), values(2).toInt)
        }
      }).keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(String, String, Int), (String, String, Int)] {


      private var valueState: ValueState[(String, String, Int)] = _

      override def open(parameters: Configuration): Unit = {
          val stateDescriptor = new ValueStateDescriptor[(String, String, Int)]("CheckpointTest", classOf[(String,String,Int)])
          valueState = getRuntimeContext.getState(stateDescriptor)
      }

      override def flatMap(in: (String, String, Int), collector: Collector[(String, String, Int)]): Unit = {
        val stateData = valueState.value()
        if (stateData != null && stateData._1.equals(in._1)) {
          val tuple = (in._1, in._2, in._3 + stateData._3)
          println(tuple)
          collector.collect(tuple)
          valueState.update(tuple)
        } else {
          collector.collect(in)
          valueState.update(in)
        }

      }
    })

    convertedData.print()

    env.execute("Checkpoint test")


  }

}
