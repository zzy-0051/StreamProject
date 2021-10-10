package alert;

import model.LogEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import schema.LogSchema;
import util.ExecutionEnvUtil;
import util.KafkaConfigUtil;

import java.util.Properties;

/**
 * @Author : zzy
 * @Date : 2021/10/08
 */

public class LogEventAlert {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        FlinkKafkaConsumer<LogEvent> consumer = new FlinkKafkaConsumer<>(
                parameterTool.get("log.topic"),
                new LogSchema(),
                properties);
        env.addSource(consumer)
                .filter(logEvent -> "error".equals(logEvent.getLevel()))
                .print();
        env.execute("log event alert");
    }
}
