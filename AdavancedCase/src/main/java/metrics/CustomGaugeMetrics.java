package metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class CustomGaugeMetrics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
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
        }).map(new RichMapFunction<String, String>() {
            private transient int tag = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().getMetricGroup().addGroup("metric_demo").gauge("gauge_test", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return tag;
                    }
                });
            }

            @Override
            public String map(String value) throws Exception {
                tag++;
                System.out.println("tag : " + tag);
                return value;
            }
        }).print();

        env.execute("custom gauge metrics");
    }
}
