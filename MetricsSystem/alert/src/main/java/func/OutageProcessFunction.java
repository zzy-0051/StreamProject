package func;

import lombok.extern.slf4j.Slf4j;
import model.OutageMetricEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author : zzy
 * @Date : 2021/10/08
 */

@Slf4j
public class OutageProcessFunction extends KeyedProcessFunction<String, OutageMetricEvent,OutageMetricEvent> {

    private ValueState<OutageMetricEvent> outageMetricState;
    private ValueState<Boolean> recover;

    private int delay;
    private int alertCountLimit;

    public OutageProcessFunction(int delay, int alertCountLimit) {
        this.delay = delay;
        this.alertCountLimit = alertCountLimit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<OutageMetricEvent> outageInfo = TypeInformation.of(new TypeHint<OutageMetricEvent>() {
        });
        TypeInformation<Boolean> recoverInfo = TypeInformation.of(new TypeHint<Boolean>() {
        });
        outageMetricState = getRuntimeContext().getState(new ValueStateDescriptor<>("outage_info", outageInfo));
        recover = getRuntimeContext().getState(new ValueStateDescriptor<>("recover_info", recoverInfo));
    }

    @Override
    public void processElement(OutageMetricEvent outageMetricEvent, Context ctx, Collector<OutageMetricEvent> collector) throws Exception {
        OutageMetricEvent current = outageMetricState.value();
        if (current == null) {
            current = new OutageMetricEvent(outageMetricEvent.getClusterName(), outageMetricEvent.getHostIp(),
                    outageMetricEvent.getTimestamp(), outageMetricEvent.getRecover(), System.currentTimeMillis());
        } else {
            if (outageMetricEvent.getLoad5() != null) {
                current.setLoad5(outageMetricEvent.getLoad5());
            }
            if (outageMetricEvent.getCpuUsePercent() != null) {
                current.setCpuUsePercent(outageMetricEvent.getCpuUsePercent());
            }
            if (outageMetricEvent.getMemUsedPercent() != null) {
                current.setMemUsedPercent(outageMetricEvent.getMemUsedPercent());
            }
            if (outageMetricEvent.getSwapUsedPercent() != null) {
                current.setSwapUsedPercent(outageMetricEvent.getSwapUsedPercent());
            }
            current.setSystemTimestamp(System.currentTimeMillis());
        }

        if (recover.value() != null && !recover.value() && outageMetricEvent.getTimestamp() > current.getTimestamp()) {
            OutageMetricEvent recoverEvent = new OutageMetricEvent(outageMetricEvent.getClusterName(), outageMetricEvent.getHostIp(),
                    current.getTimestamp(), true, System.currentTimeMillis());
            recoverEvent.setRecoverTime(ctx.timestamp());
            log.info("recoverEvent : {}", recoverEvent);
            collector.collect(recoverEvent);
            current.setCounter(0);
            outageMetricState.update(current);
            recover.update(true);
        }

        current.setTimestamp(outageMetricEvent.getTimestamp());
        outageMetricState.update(current);
        ctx.timerService().registerEventTimeTimer(current.getSystemTimestamp() + delay);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutageMetricEvent> out) throws Exception {
        OutageMetricEvent result = outageMetricState.value();

        if (result != null && timestamp >= result.getSystemTimestamp() + delay && System.currentTimeMillis() - result.getTimestamp() >= delay) {
            if (result.getCounter() > alertCountLimit) {
                log.info("alert count:{} :{}", alertCountLimit, result);
                return;
            }
            log.info("alert:timestamp = {}, result = {}", System.currentTimeMillis(), result);
            result.setRecover(false);
            out.collect(result);
            ctx.timerService().registerEventTimeTimer(timestamp + delay);
            result.setCounter(result.getCounter() + 1);
            result.setSystemTimestamp(timestamp);
            outageMetricState.update(result);
            recover.update(false);
        }
    }
}
