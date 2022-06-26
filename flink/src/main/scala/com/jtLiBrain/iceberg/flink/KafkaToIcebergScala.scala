package com.jtLiBrain.iceberg.flink

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Schema, TableSchema}
import org.apache.flink.table.data.{StringData, TimestampData}
import org.apache.flink.table.types.logical.{RawType, TimestampType, VarCharType}
import org.apache.flink.table.types.{AtomicDataType, DataType}
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.sink.FlinkSink

/**
 * 不能插入Iceberg
 */
object KafkaToIcebergScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)

    val source = KafkaSource.builder[String]
      .setBootstrapServers("localhost:9092")
      .setTopics("iceber-events")
      .setGroupId("my-group-2")
      .setStartingOffsets(OffsetsInitializer.latest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build

    val input = env.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")

    val output = input.map(s => {
        val jo = JSON.parseObject(s)

        val ts = jo.getTimestamp("ts")
        val user = jo.getString("userName")
        val page = jo.getString("page")
        val date = jo.getDate("ts")

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        Row.of(
          TimestampData.fromTimestamp(ts),
          StringData.fromString(user),
          StringData.fromString(page),
          StringData.fromString(sdf.format(date))
        )
    })

    val tableLoader = TableLoader.fromHadoopTable(
      "file:///Users/dream/Env/iceberg/flink_warehouse/iceberg_db/user_visit",
      new Configuration()
    )

    val tableSchema = TableSchema.builder()
      .field("ts", new AtomicDataType(new TimestampType()))
      .field("user_name", new AtomicDataType(new VarCharType()))
      .field("page", new AtomicDataType(new VarCharType()))
      .field("pn", new AtomicDataType(new VarCharType()))
      .build()

    FlinkSink.forRow(output.javaStream, tableSchema)
      .tableLoader(tableLoader)
      .build

    env.execute("Kafka to Iceberg")
  }
}
