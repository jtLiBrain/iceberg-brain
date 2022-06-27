package com.jtLiBrain.iceberg.flink

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.data.StringData
import org.apache.flink.table.types.logical.{BigIntType, VarCharType}
import org.apache.flink.table.types.AtomicDataType
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
      .setTopics("user_behavior")
      .setGroupId("my-group-1")
      .setStartingOffsets(OffsetsInitializer.latest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build

    val input = env.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")

    val output = input.map(s => {
        val jo = JSON.parseObject(s)

        val user = jo.getString("userName")
        val page = jo.getString("page")
        val ts = jo.getLong("ts")
        val pn = jo.getString("pn")

        Row.of(
          StringData.fromString(user),
          StringData.fromString(page),
          ts,
          StringData.fromString(pn))
    })

    val tableLoader = TableLoader.fromHadoopTable(
      "file:///Users/dream/Env/iceberg/flink_warehouse/iceberg_db/user_behavior",
      new Configuration()
    )

    val tableSchema = TableSchema.builder()
      .field("user_name", new AtomicDataType(new VarCharType()))
      .field("page", new AtomicDataType(new VarCharType()))
      .field("ts", new AtomicDataType(new BigIntType()))
      .field("pn", new AtomicDataType(new VarCharType()))
      .build()

    FlinkSink.forRow(output.javaStream, tableSchema)
      .tableLoader(tableLoader)
      .build

    env.execute("Kafka to Iceberg")
  }
}
