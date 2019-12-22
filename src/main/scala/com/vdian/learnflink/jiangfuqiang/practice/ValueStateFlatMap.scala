package com.vdian.learnflink.jiangfuqiang.practice

import com.vdian.learnflink.jiangfuqiang.practice.ClassObject.PersonSalary
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * author: jiangfuqiang
  * date:2019/12/20
  *
  */
class ValueStateFlatMap extends RichFlatMapFunction[PersonSalary, PersonSalary]{

  private var valueState: ValueState[PersonSalary] = _

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ValueStateDescriptor[PersonSalary]("stateTest", classOf[PersonSalary])
    valueState = getRuntimeContext.getState(stateDescriptor)
  }

  override def flatMap(in: PersonSalary, collector: Collector[PersonSalary]): Unit = {
    val value: PersonSalary = valueState.value()
    if (value == null) {

      collector.collect(in)
      valueState.update(in)
    } else {
      value.salary = value.salary + in.salary
      val ps = value
      collector.collect(ps)
      valueState.update(value)

    }
  }
}
