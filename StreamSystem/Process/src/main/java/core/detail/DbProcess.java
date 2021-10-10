package core.detail;

import bean.ConfigTableBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import core.func.ConfigTableFunction;
import core.func.CustomDebeziumDeserializationSchema;
import core.func.DimensionSinkFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.KafkaUtil;

import javax.annotation.Nullable;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class DbProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "ods_base_db";
        String groupId = "ods_base_db_group";
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env
                .addSource(KafkaUtil.getKafkaConsumer(topic, groupId))
                .map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {

                String data = value.getString("data");

                return !(data == null || data.length() <= 2);
            }
        });

        filterDS.print();

        DebeziumSourceFunction<String> configSourceFunction = MySqlSource.<String>builder()
                .hostname("znode2")
                .port(3306)
                .username("root")
                .password("ZengZhenyu0622")
                .databaseList("stream_process_config")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> configTableDS = env.addSource(configSourceFunction);

        MapStateDescriptor<String, ConfigTableBean> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, ConfigTableBean.class);

        BroadcastStream<String> broadcastStream = configTableDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(ConfigTableBean.SINK_TYPE_HBASE) {};

        SingleOutputStreamOperator<JSONObject> kafkaDS = connectDS.process(new ConfigTableFunction(hbaseTag, mapStateDescriptor));
        kafkaDS.print("Kafka >>> ");

        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print("HBase >>> ");

        hbaseDS.addSink(new DimensionSinkFunction());

        kafkaDS.addSink(KafkaUtil.getKafkaProducerWithSchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        }));

        env.execute();
    }
}
