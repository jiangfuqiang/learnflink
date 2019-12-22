package com.vdian.learnflink.jiangfuqiang.practice

import java.sql.Timestamp
import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps

class OrderTableSource extends StreamTableSource[Row] with DefinedRowtimeAttributes{
  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val dataStream = execEnv.socketTextStream("localhost", 9090)
      .map(new MapFunction[String, Row]{
        override def map(t: String): Row = {
            val values = t.split(" ")
            var row = new Row(4)
            row.setField(0, values(0).toInt)
            row.setField(1, values(1).toInt)
            row.setField(2, values(2).toInt)
            row.setField(3, new Timestamp(values(3).toLong))
            row
        }
      })

    dataStream
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    val rowtimeAttr = new RowtimeAttributeDescriptor("rowtime", new ExistingField("rowtime"), new AscendingTimestamps)
    val listRowtimeAttrDesc = Collections.singletonList(rowtimeAttr)
    listRowtimeAttrDesc
  }

  override def getReturnType: TypeInformation[Row] = {
    val names = Array[String]("a", "b", "c", "rowtime")
    val types = Array[TypeInformation[_]](Types.INT, Types.INT,Types.INT, Types.SQL_TIMESTAMP)
    val rti = new RowTypeInfo(types, names)
    rti
  }

  override def getTableSchema: TableSchema = {
    val names = Array[String]("a", "b", "c", "rowtime")
    val types = Array[TypeInformation[_]](Types.INT, Types.INT,Types.INT, Types.SQL_TIMESTAMP)
    val ts = new TableSchema(names, types)
    ts
  }
}
