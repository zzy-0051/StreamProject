package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author : zzy
 * @Date : 2021/09/27
 */

public class KafkaUtil {

    private static final String BOOTSTRAP_SERVER = "zmaster:9092,znode1:9092,znode2:9092";

    private static final String DWD_DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                BOOTSTRAP_SERVER,
                topic,
                new SimpleStringSchema()
        );
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducerWithSchema(KafkaSerializationSchema<T> serializationSchema) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        return new FlinkKafkaProducer<T>(
                DWD_DEFAULT_TOPIC,
                serializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    public static String getKafkaDDL(String topic, String groupId) {
        return
                ") WITH ( "
                        + "  'connector' = 'kafka', "
                        + "  'topic' = '" + topic + "', "
                        + "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVER + "', "
                        + "  'properties.group.id' = '" + groupId + "', "
                        + "  'format' = 'json' "
                        + ")"
                ;
    }
}
