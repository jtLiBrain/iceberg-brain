package com.jtLiBrain.iceberg.flink

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object KafkaToIcebergSqlScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)

    val tEnv = StreamTableEnvironment.create(env)

    tEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE user_behavior_kafka (
        |  `userName` STRING,
        |  `page` STRING,
        |  `ts` BIGINT,
        |  `pn` STRING
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'user_behavior',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin)

    tEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE user_behavior_iceberg (
        |  `user_name` STRING,
        |  `page` STRING,
        |  `ts` BIGINT,
        |  `pn` STRING
        |) PARTITIONED BY (pn)
        |WITH (
        |  'connector' = 'iceberg',
        |  'catalog-type' = 'hadoop',
        |  'catalog-name' = 'hadoop_catalog',
        |  'catalog-database' = 'iceberg_db',
        |  'catalog-table' = 'user_behavior',
        |  'warehouse'='file:///Users/dream/Env/iceberg/flink_warehouse'
        |)
        |""".stripMargin)

    val ubTable = tEnv.from("user_behavior_kafka").select(
      $("userName").as("user_name"),
      $("page").as("page"),
      $("ts").as("ts"),
      $("pn").as("pn")
    )

    ubTable.executeInsert("user_behavior_iceberg")
  }
}
