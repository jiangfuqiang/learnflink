package com.vdian.learnflink.jiangfuqiang.watermark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * author: jiangfuqiang
  * date:2019/12/17
  * 水位线测试类
  */
object WatermarkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputData: DataStream[String] = env.socketTextStream("localhost", 9090)
    val convertedStream: DataStream[(Int, String, Long)] = inputData.filter(_.nonEmpty)
//      .map(new MapFunction[String, (Int, String, Long)]{
//      override def map(t: String): (Int, String, Long) = {
//        val values = t.split(" ")
//        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        val dateTime = sdf.format(new Date(values(2).toLong))
//        println("> " + values(0) + " " + values(1) + " " + dateTime)
//        (values(0).toInt, values(1), values(2).toLong)
//      }
//    })
      .flatMap(new FlatMapFunction[String, (Int, String,Long)] {
      override def flatMap(t: String, collector: Collector[(Int, String, Long)]): Unit = {
        val values = t.split(" ")
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateTime = sdf.format(new Date(values(2).toLong))
        println("> " + values(0) + " " + values(1) + " " + dateTime + " = " + values(2))
        collector.collect(values(0).toInt, values(1), values(2).toLong)
      }
    })
    val watermarkStream = convertedStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Int, String, Long)] {

      var expireTime = 2000L;
      var maxWatermarkTime = 0L
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      override def getCurrentWatermark: Watermark = {
        val time = maxWatermarkTime - expireTime

        new Watermark(maxWatermarkTime - expireTime)
      }

      override def extractTimestamp(t: (Int, String, Long), l: Long): Long = {
        maxWatermarkTime = Math.max(maxWatermarkTime, t._3)
        if(maxWatermarkTime > 0) {
          println("watermarkTime is "+sdf.format(new Date(maxWatermarkTime)))
        }
        t._3
      }
    }).keyBy(_._1)

    val windowStream = watermarkStream.timeWindow(Time.seconds(5))
      .process(new CustomWindowFunction)
    windowStream.print()

    env.execute("watermarkTest")

  }


}
