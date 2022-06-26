```sql
CREATE TABLE hadoop_catalog.iceberg_db.user_visit (
    ts TIMESTAMP,
    user_name STRING,
    page STRING,
    pn  STRING
) PARTITIONED BY (pn);
```

```
./bin/sql-client.sh embedded -j ~/Env/iceberg/iceberg-flink-runtime-1.14-0.13.2.jar shell
```

```
CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='file:///Users/dream/Env/iceberg/flink_warehouse',
  'property-version'='1'
)

USE CATALOG hadoop_catalog
```

bin/kafka-topics.sh --create --topic iceber-events --bootstrap-server localhost:9092