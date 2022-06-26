package com.jtLiBrain.iceberg.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class KafkaToIceberg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new org.apache.flink.configuration.Configuration());
        // 必须启用checkpoint
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iceber-events")
                .setGroupId("my-group-1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<RowData> output = input.map(s -> {
            JSONObject jo = JSON.parseObject(s);

            Timestamp ts = jo.getTimestamp("ts");
            String user = jo.getString("userName");
            String page = jo.getString("page");
            Date date = jo.getDate("ts");

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            return GenericRowData.of(
                    TimestampData.fromTimestamp(ts),
                    StringData.fromString(user),
                    StringData.fromString(page),
                    StringData.fromString(sdf.format(date)));
        });

        TableLoader tableLoader = TableLoader.fromHadoopTable(
                    "file:///Users/dream/Env/iceberg/flink_warehouse/iceberg_db/user_visit",
                    new Configuration()
                );

        FlinkSink.forRowData(output)
                .tableLoader(tableLoader)
                .build();

        env.execute("Kafka to Iceberg");
    }
}
