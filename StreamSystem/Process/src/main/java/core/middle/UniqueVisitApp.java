package core.middle;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.KafkaUtil;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    //  env.enableCheckpointing(5000L);
    //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/cdc/ck"));

        String groupId = "unique_visit_app_group";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        SingleOutputStreamOperator<JSONObject> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .map(JSONObject::parseObject);

        KeyedStream<JSONObject, String> keyedStream = kafkaDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stringValueStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stringValueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                String lastPage = value.getJSONObject("page").getString("last_page_id");

                if (lastPage == null || lastPage.length() <= 0) {

                    String lastDate = valueState.value();
                    String date = sdf.format(value.getLong("ts"));

                    if (lastDate == null || !lastDate.equals(date)) {

                        valueState.update(date);

                        return true;

                    } else {

                        return false;
                    }

                } else {

                    return false;

                }
            }
        });

        filterDS.print(">>>>>>>>>");
        filterDS.map(data -> JSONObject.toJSONString(data));
        filterDS.map(JSONAware::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        env.execute();
    }
}
