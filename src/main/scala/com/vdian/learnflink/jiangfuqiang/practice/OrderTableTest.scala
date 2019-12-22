package com.vdian.learnflink.jiangfuqiang.practice


import java.util

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.ConnectorDescriptor
import org.apache.flink.types.Row

object OrderTableTest {

  def main(args: Array[String]): Unit= {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      val tableEnv = StreamTableEnvironment.create(env)


      tableEnv.registerTableSource("Orders", new OrderTableSource)
      val order: Table = tableEnv.scan("Orders")

      val result:Table = order
        .select('a, 'b, 'rowtime)
        .window(Tumble over 5.second on 'rowtime as 'hw)
        .groupBy('hw, 'a)
        .select('a, 'hw.end as 'hwe, 'b.avg as 'ab)

      val resultData = tableEnv.toRetractStream[Row](result)
      resultData.print()
      env.execute("OrderTableTest")
  }

}
