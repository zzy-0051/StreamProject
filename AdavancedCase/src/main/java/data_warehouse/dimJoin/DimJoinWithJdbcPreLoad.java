package data_warehouse.dimJoin;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author : zzy
 * @Date : 2021/10/10
 */

public class DimJoinWithJdbcPreLoad {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //  ...
            }

            @Override
            public void cancel() {
                //  ...
            }
        });

        source.flatMap(new RichFlatMapFunction<String, String>() {
            Map<String,String> dimInfo = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                //  jdbc get connection ...

                //  resultSet ===> dimInfo
            }

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //  dim_join    ...
            }
        });

        //  ...
    }
}
