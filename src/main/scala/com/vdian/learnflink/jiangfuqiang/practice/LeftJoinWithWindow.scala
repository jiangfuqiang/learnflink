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

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 9090)
    val inputStream2: DataStream[String] = env.socketTextStream("localhost", 9091)

    val convertedStream1: DataStream[Person] = inputStream1.filter(_.nonEmpty).map(new MapFunction[String, Person] {
      override def map(t: String): Person = {
        val values = t.split(" ")
        val p = new Person()
        p.name = values(0)
        p.pid = values(1).toInt
        p.age = values(2).toInt
        p.time = values(3).toLong
        p
      }
    })

    val convertedStream2: DataStream[Salary] = inputStream2.filter(_.nonEmpty).map(new MapFunction[String, Salary] {
      override def map(t: String): Salary = {
        val values = t.split(" ")
        val s = new Salary()
        s.sid = values(0).toInt
        s.salary = values(1).toInt
        s.stime = values(2).toLong
        s
      }
    })

    val person = tableEnv.fromDataStream( convertedStream1, 'name, 'pid, 'age, 'time.rowtime)
    val salary = tableEnv.fromDataStream( convertedStream2, 'sid, 'salary, 'stime.rowtime)



    val result = person.join(salary)
        .where( 'pid === 'sid && 'time > 'stime - 10.seconds && 'time < 'stime + 5.seconds)
        .select('name, 'pid, 'age, 'salary)


      .toAppendStream[Row].map(new MapFunction[Row, PersonSalary]{
      override def map(row: Row): PersonSalary = {
        val ps = new PersonSalary
        ps.name = row.getField(0).asInstanceOf[String]
        ps.id = row.getField(1).asInstanceOf[Int]
        ps.age = row.getField(2).asInstanceOf[Int]
        ps.salary = row.getField(3).asInstanceOf[Int]
        if(row.getField(4) != null) {

          ps.time = row.getField(4).asInstanceOf[Long]
        }
        ps
      }
    })

      .keyBy(_.id)
      .flatMap(new ValueStateFlatMap)


    result.print()

    env.execute("LeftJoinWithWindow")

  }

}
