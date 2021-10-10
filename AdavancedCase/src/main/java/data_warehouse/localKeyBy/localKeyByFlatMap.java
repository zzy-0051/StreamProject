package data_warehouse.localKeyBy;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @Author : zzy
 * @Date : 2021/10/10
 */

public class localKeyByFlatMap extends RichFlatMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {

    private ListState<Tuple2<String, Long>> localState;

    //  local_buffer
    private HashMap<String, Long> localBuffer;

    private int batchSize;

    private AtomicInteger currentSize;

    private int subtaskIndex;

    public localKeyByFlatMap(int batchSize) {
        checkArgument(batchSize >= 0, "Cannot define a negative value for the batchSize.");
        this.batchSize = batchSize;
    }

    @Override
    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
        Long value = localBuffer.get(in);
        if (value == null) {
            localBuffer.put(in, 1L);
        } else {
            localBuffer.put(in, value + 1L);
        }
        System.out.println("invoke subtask : " + subtaskIndex + " appId : " + in + " pv : " + localBuffer.get(in));

        if (currentSize.incrementAndGet() >= batchSize) {
            for (Map.Entry<String, Long> appStat : localBuffer.entrySet()) {
                out.collect(Tuple2.of(appStat.getKey(), appStat.getValue()));
                System.out.println("batchSend subtask : " + subtaskIndex + " appId : " + appStat.getKey() + " pv : " + appStat.getValue());
            }
        }
        localBuffer.clear();
        currentSize.set(0);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        localState.clear();
        for (Map.Entry<String, Long> appStat : localBuffer.entrySet()) {
            localState.add(Tuple2.of(appStat.getKey(), appStat.getValue()));
            System.out.println("snapshot subtask : " + subtaskIndex + " appId : " + appStat.getKey() + " pv : " + appStat.getValue());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // restore buffer_data
        localState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("localState",
                        TypeInformation.of(
                                new TypeHint<Tuple2<String, Long>>() {
                                }
                        )
                )
        );

        localBuffer = new HashMap();

        if (context.isRestored()) {
            for (Tuple2<String, Long> appIdStat : localState.get()) {
                Long value = localBuffer.get(appIdStat.f0);
                if (value == null) {
                    localBuffer.put(appIdStat.f0, appIdStat.f1);
                } else {
                    localBuffer.put(appIdStat.f0, value + appIdStat.f1);
                }
                System.out.println("init subtask : " + subtaskIndex + " appId : " + appIdStat.f0 + " value : " + appIdStat.f1);
            }
            currentSize = new AtomicInteger(batchSize);
        } else {
            currentSize = new AtomicInteger(0);
        }
    }
}
