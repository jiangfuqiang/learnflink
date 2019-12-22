package com.vdian.learnflink.jiangfuqiang.practice

import java.lang.reflect.Field
import java.sql.Timestamp
import java.util
import java.util.Collections

import com.vdian.learnflink.jiangfuqiang.practice.ClassObject.Person
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

class PersonTableSource extends StreamTableSource[Person] with DefinedRowtimeAttributes{
  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Person] = {
    val inputData: DataStream[String] = execEnv.socketTextStream("localhost", 9090)
    val resultData = inputData.map(new MapFunction[String, Person]{
      override def map(t: String): Person = {
        val values = t.split(" ")

        val row = new Person
        row.pid = values(0).toInt
        row.salary = values(1).toInt
        row.time = values(2).toLong
        row
      }
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Person] {
      var currentTimeStam = 0L
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStam)
      }

      override def extractTimestamp(t: Person, l: Long): Long = {
        currentTimeStam = t.time
        t.time
      }
    })

    resultData
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    val rowtimeAttr = new RowtimeAttributeDescriptor("time", new ExistingField("time"), new AscendingTimestamps)
    val listRowtimeAttrDesc = Collections.singletonList(rowtimeAttr)
    listRowtimeAttrDesc
  }

  override def getReturnType: TypeInformation[Person] = {

    val tp = createTypeInformation[Person]
    tp

  }

  override def getTableSchema: TableSchema = {
    val names = Array[String]("pid", "salary", "time")
    val types = Array[TypeInformation[_]](Types.INT, Types.INT, Types.SQL_TIMESTAMP)
    val tableSchema = new TableSchema(names, types)
    tableSchema
  }
}
