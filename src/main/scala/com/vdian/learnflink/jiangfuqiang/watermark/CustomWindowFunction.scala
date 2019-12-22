package com.vdian.learnflink.jiangfuqiang.watermark

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author: jiangfuqiang
  * date:2019/12/19
  *
  */
class CustomWindowFunction extends ProcessWindowFunction[(Int, String, Long), (String, Long), Int, TimeWindow] {
  override def process(key: Int, context: Context, elements: Iterable[(Int, String, Long)], out: Collector[(String, Long)]): Unit = {
    val start = context.window.getStart
    val end = context.window.getEnd
    println(start + " -> " + end)
    elements.foreach(t => {
      out.collect((t._2, t._3))
    })
  }
}