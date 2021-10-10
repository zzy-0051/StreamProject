package data_warehouse.dimJoin;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @Author : zzy
 * @Date : 2021/10/09
 */

public class DimJoinWithBroadcastStream {

    public static void main(String[] args) {
        MapStateDescriptor<String, String> broadcastStateDesc = new MapStateDescriptor<>("broadcast_state", String.class, String.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> broadCastStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //  ...
            }

            @Override
            public void cancel() {
                //  ...
            }
        });

        DataStreamSource<String> inputStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //  ...
            }

            @Override
            public void cancel() {
                //  ...
            }
        });

        broadCastStream.broadcast(broadcastStateDesc);

//        inputStream.connect(broadCastStream).process(
//                //  ..
//        );
    }
}
