import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : zzy
 * @Date : 2021/09/27
 */

public class CaptureWithCustomDeserialization {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("znode2")
                .port(3306)
                .username("root")
                .password("ZengZhenyu0622")
                .databaseList("stream_test")
                .tableList("stream_test.user")
                .deserializer(new CustomDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();
    }
}
