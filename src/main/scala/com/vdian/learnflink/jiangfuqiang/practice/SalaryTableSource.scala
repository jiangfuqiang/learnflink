package com.vdian.learnflink.jiangfuqiang.practice

import java.util
import java.util.Collections

import com.vdian.learnflink.jiangfuqiang.practice.ClassObject.Salary
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row

class SalaryTableSource extends StreamTableSource[Row] with DefinedRowtimeAttributes{
  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val inputData: DataStream[String] = execEnv.socketTextStream("localhost", 9091)
    val resultData = inputData.map(new MapFunction[String, Row]{
      override def map(t: String): Row = {
        val values = t.split(" ")
        val row = new Row(3)
        row.setField(0, values(0).toInt)
        row.setField(1, values(1).toInt)
        row.setField(2, values(2).toLong)
        row
      }
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Row] {
      var currentTimeStam = 0L
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStam)
      }

      override def extractTimestamp(t: Row, l: Long): Long = {
        currentTimeStam = t.getField(2).asInstanceOf[Long]
        t.getField(2).asInstanceOf[Long]
      }
    })

    resultData
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    val rowtimeAttr = new RowtimeAttributeDescriptor("stime", new ExistingField("stime"), new AscendingTimestamps)
    val listRowtimeAttrDesc = Collections.singletonList(rowtimeAttr)
    listRowtimeAttrDesc
  }

  override def getReturnType: TypeInformation[Row] = {
    val names = Array[String]("sid", "salary", "stime")
    val types = Array[TypeInformation[_]](Types.INT, Types.INT, Types.SQL_TIMESTAMP)
    Types.ROW(names, types)
  }

  override def getTableSchema: TableSchema = {
    val names = Array[String]("sid", "salary", "stime")
    val types = Array[TypeInformation[_]](Types.INT, Types.INT, Types.SQL_TIMESTAMP)
    val tableSchema = new TableSchema(names, types)
    tableSchema
  }
}
