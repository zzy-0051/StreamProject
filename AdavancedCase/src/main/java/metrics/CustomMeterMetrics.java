package metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class CustomMeterMetrics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);
        env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> out) throws Exception {
                while (isRunning) {
                    out.collect(String.valueOf(Math.round(Math.random() * 100)));
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).map(new RichMapFunction<String, String>() {
            Meter meter;
            int index;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
                index = getRuntimeContext().getIndexOfThisSubtask() + 1;
                meter = getRuntimeContext().getMetricGroup().addGroup("metrics_demo").meter("meter_test",new DropwizardMeterWrapper(dropwizardMeter));
            }

            @Override
            public String map(String value) throws Exception {
                meter.markEvent();
                System.out.println("index : " + index + " , rate : " + meter.getRate() + " , count : " + meter.getCount());
                return value;
            }
        }).print();

        env.execute("Custom Meter Metrics");
    }
}
