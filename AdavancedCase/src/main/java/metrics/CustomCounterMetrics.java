package metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class CustomCounterMetrics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(4);

        env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect(String.valueOf(Math.round(Math.random() * 100)));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        })/*.setParallelism(2).slotSharingGroup("process")*/.process(new ProcessFunction<String, String>() {
            Counter counter;
            int index;

            @Override
            public void open(Configuration parameters) throws Exception {
                counter = getRuntimeContext().getMetricGroup().addGroup("metrics_demo").counter("process_test");
                index = getRuntimeContext().getIndexOfThisSubtask() + 1;
                super.open(parameters);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                counter.inc();
                System.out.println("process_index : " + (index + 1) + " , process_counter : " + counter.getCount());
                out.collect(value);
            }
        }).map(new RichMapFunction<String, String>() {
            Counter counter;
            int index;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                index = getRuntimeContext().getIndexOfThisSubtask();
                counter = getRuntimeContext().getMetricGroup().addGroup("metrics_test").counter("map_test" + index);
            }

            @Override
            public String map(String value) throws Exception {
                System.out.println("map_index : " + (index + 1) + " , map_counter : " + counter.getCount());
                counter.inc();
                if ("90".equals(value)) {
                    System.out.println("You can call the counter in the if_clause");
                }
                return value;
            }
        }).print();

        env.execute("Custom Counter Metrics Job");
    }
}
