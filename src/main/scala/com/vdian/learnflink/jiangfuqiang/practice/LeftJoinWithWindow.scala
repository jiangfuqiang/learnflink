package com.vdian.learnflink.jiangfuqiang.practice

import com.vdian.learnflink.jiangfuqiang.practice.ClassObject.{Person, PersonSalary, Salary}
import org.apache.flink.api.common.functions.{JoinFunction, MapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.OverWindow
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * author: jiangfuqiang
  * date:2019/12/20
  *
  */
object LeftJoinWithWindow {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)



    tableEnv.registerTableSource("person", new PersonTableSource)
//    tableEnv.registerTableSource("salary", new SalaryTableSource)
    val person = tableEnv.scan("person")
//    val salary = tableEnv.scan("salary")



    val result = person
//      .join(salary,'pid === 'sid)
//      .window(Tumble over 5.second on 'time as 'w)
        .groupBy('pid)
        .select('pid, 'salary.sum as 'salary)

      .toRetractStream[Row].map(new MapFunction[(Boolean, Row), PersonSalary] {
        override def map(t: (Boolean, Row)): PersonSalary = {
          val row = t._2
          val ps = new PersonSalary
          ps.id = row.getField(0).asInstanceOf[Int]
          ps.salary = row.getField(1).asInstanceOf[Int]
          ps
        }
      })

//      .keyBy(_.id)
//      .flatMap(new ValueStateFlatMap)


    result.print()

    env.execute("LeftJoinWithWindow")

  }

}
