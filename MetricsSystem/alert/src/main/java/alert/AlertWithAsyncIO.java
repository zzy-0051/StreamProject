package alert;

import func.AlertRuleAsyncIOFunc;
import model.AlertEvent;
import model.MetricEvent;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import schema.MetricSchema;
import util.ExecutionEnvUtil;
import util.KafkaConfigUtil;
import watermark.MetricWatermarkStrategy;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class AlertWithAsyncIO {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        FlinkKafkaConsumer<MetricEvent> consumer = new FlinkKafkaConsumer<>(
                parameterTool.get("metrics.topic"),
                new MetricSchema(),
                properties
        );
        SingleOutputStreamOperator<MetricEvent> machineDS = env.addSource(consumer).assignTimestampsAndWatermarks(new MetricWatermarkStrategy());
        AsyncDataStream.unorderedWait(machineDS,new AlertRuleAsyncIOFunc(),1000, TimeUnit.MICROSECONDS,100)
                .map(new MapFunction<MetricEvent, AlertEvent>() {
                    @Override
                    public AlertEvent map(MetricEvent metricEvent) throws Exception {
                        List<String> xxx = (List<String>) metricEvent.getFields().get("xxx");
                        AlertEvent alertEvent = new AlertEvent();
                        alertEvent.setType(metricEvent.getName());
                        alertEvent.setTriggerTime(metricEvent.getTimestamp());
                        alertEvent.setMetricEvent(metricEvent);
                        if (metricEvent.getTags().get("recover") != null
                                && Boolean.parseBoolean(metricEvent.getTags().get("recover"))) {
                            alertEvent.setRecover(true);
                            alertEvent.setRecoverTime(metricEvent.getTimestamp());
                        } else {
                            alertEvent.setRecover(false);
                        }
                        return alertEvent;
                    }
                }).print();

        env.execute("AsyncIO Job");
    }
}
