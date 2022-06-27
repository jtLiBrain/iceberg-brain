# 1. 启动Flink集合和客户端
```shell
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
./bin/start-cluster.sh
```

```shell
./bin/sql-client.sh embedded -j ~/Env/iceberg/iceberg-flink-runtime-1.14-0.13.2.jar shell
```

# 2. 建表
```sql
CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='file:///Users/dream/Env/iceberg/flink_warehouse',
  'property-version'='1'
);

USE CATALOG hadoop_catalog;

CREATE DATABASE iceberg_db;

CREATE TABLE hadoop_catalog.iceberg_db.user_behavior (
    user_name STRING,
    page STRING,
    ts BIGINT,
    pn STRING
) PARTITIONED BY (pn);
```

# 3. Kafka
```shell
bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --topic user_behavior --bootstrap-server localhost:9092

bin/kafka-console-producer.sh --topic user_behavior --bootstrap-server localhost:9092
```

> {"userName":"a", "page":"list", "ts":1656341470000, "pn":"2022-06-27"}

# 4. Flink
```sql
set 'sql-client.execution.result-mode' = 'tableau';
```
