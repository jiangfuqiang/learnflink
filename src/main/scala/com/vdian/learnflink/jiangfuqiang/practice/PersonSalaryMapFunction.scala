package com.vdian.learnflink.jiangfuqiang.practice

import com.vdian.learnflink.jiangfuqiang.practice.ClassObject.PersonSalary
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

/**
  * author: jiangfuqiang
  * date:2019/12/20
  *
  */
class PersonSalaryMapFunction extends MapFunction[(Boolean, Row), PersonSalary]{
  override def map(t: (Boolean, Row)): PersonSalary = {
    val row = t._2
    val ps = new PersonSalary
    ps.name = row.getField(0).asInstanceOf[String]
    ps.id = row.getField(1).asInstanceOf[Int]
    ps.age = row.getField(2).asInstanceOf[Int]
    ps.salary = row.getField(3).asInstanceOf[Int]
    ps
  }
}
