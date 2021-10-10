package alert;

import func.GetAlertRuleSourceFunc;
import lombok.extern.slf4j.Slf4j;
import model.AlertEvent;
import model.AlertRule;
import model.MetricEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import schema.MetricSchema;
import util.ExecutionEnvUtil;
import util.KafkaConfigUtil;
import watermark.MetricWatermarkStrategy;

import java.util.List;
import java.util.Properties;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

@Slf4j
public class UpdateAlertRuleWithBroadcast {

    private static final MapStateDescriptor<String, AlertRule> ALERT_RULE = new MapStateDescriptor<>(
            "alert_rule",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(AlertRule.class)
    );

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        FlinkKafkaConsumer<MetricEvent> consumer = new FlinkKafkaConsumer<>(
                parameterTool.get("metrics.topic"),
                new MetricSchema(),
                properties
        );

        SingleOutputStreamOperator<MetricEvent> machineData = env.addSource(consumer)
                .assignTimestampsAndWatermarks(new MetricWatermarkStrategy());

        DataStreamSource<List<AlertRule>> ruleDS = env.addSource(new GetAlertRuleSourceFunc()).setParallelism(1);

        machineData.connect(ruleDS.broadcast(ALERT_RULE)).process(new BroadcastProcessFunction<MetricEvent, List<AlertRule>, MetricEvent>() {
            @Override
            public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<MetricEvent> out) throws Exception {
                ReadOnlyBroadcastState<String, AlertRule> broadcastState = ctx.getBroadcastState(ALERT_RULE);
                if (broadcastState.contains(value.getName())) {
                    AlertRule alertRule = broadcastState.get(value.getName());
                    double used = (double) value.getFields().get(alertRule.getMeasurement());
                    if (used > Double.parseDouble(alertRule.getThresholds())) {
                        log.info("AlertRule = {}, MetricEvent = {}", alertRule, value);
                        out.collect(value);
                    }
                }
            }

            @Override
            public void processBroadcastElement(List<AlertRule> value, Context ctx, Collector<MetricEvent> out) throws Exception {
                if (value == null || value.size() == 0) {
                    return;
                }
                BroadcastState<String, AlertRule> alertRuleBroadcastState = ctx.getBroadcastState(ALERT_RULE);
                for (AlertRule aValue : value) {
                    alertRuleBroadcastState.put(aValue.getName(), aValue);
                }
            }
        }).print();

        env.execute();
    }
}
