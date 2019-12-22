package com.vdian.learnflink.jiangfuqiang.practice

/**
  * author: jiangfuqiang
  * date:2019/12/19
  *
  */
object ClassObject {
  class Person {
    var name: String = ""
    var pid: Int = _
    var age: Int = _
    var time: Long = _
  }
  class Salary {
    var sid: Int = _
    var salary: Int = 0
    var stime: Long = _
  }

  class PersonSalary {
    var name: String = ""
    var id: Int = _
    var age: Int = _
    var salary: Int = 0
    var time: Long = _

    override def toString: String = {
      val value = this.id + "_" + this.name + "_" + this.age + "_" + this.salary
      value
    }
  }
}


