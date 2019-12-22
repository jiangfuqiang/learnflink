package com.vdian.learnflink.jiangfuqiang.practice

import com.vdian.learnflink.jiangfuqiang.practice.ClassObject._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * author: jiangfuqiang
  * date:2019/12/19
  *
  */
object LeftJoinWithState {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
        p
      }
    })

    val convertedStream2: DataStream[Salary] = inputStream2.filter(_.nonEmpty).map(new MapFunction[String, Salary] {
      override def map(t: String): Salary = {
        val values = t.split(" ")
        val s = new Salary()
        s.sid = values(0).toInt
        s.salary = values(1).toInt
        s
      }
    })

    tableEnv.registerDataStream("person", convertedStream1, 'name, 'pid, 'age)
    tableEnv.registerDataStream("salary", convertedStream2, 'sid, 'salary)

    val sql = "select p.name, p.id, p.age, s.salary from person as p left join salary as s on p.pid = s.sid"
    val queryTable = tableEnv.sqlQuery(sql)

    val result = tableEnv.toRetractStream[Row](queryTable)
      .map(new PersonSalaryMapFunction).keyBy(_.id)
      .flatMap(new ValueStateFlatMap)

    result.print()

    env.execute("LeftJoinWithState")


  }

}
