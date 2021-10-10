package data_warehouse.dimJoin;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * @Author : zzy
 * @Date : 2021/10/09
 */

public class DimJoinWithDistributedCache {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("...","dim");
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
            @Override
            public void open(Configuration parameters) throws Exception {
                DistributedCache cache = getRuntimeContext().getDistributedCache();
                File dim = cache.getFile("dim");
                //  ...
            }

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //  ...
            }
        });

        //  ...
    }
}
