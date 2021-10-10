package core.middle;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import utils.KafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

    //   env.enableCheckpointing(5000L);
    //   env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //   env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/cdc/ck"));

        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp_group";
        String sinkTopic = "dwm_user_jump_detail";
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

//        jsonObjWithWMDS.print("jsonObjWithWMDS>>>>>>");

        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPage = value.getJSONObject("page").getString("last_page_id");
                return lastPage == null || lastPage.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPage = value.getJSONObject("page").getString("last_page_id");
                return lastPage == null || lastPage.length() <= 0;
            }
        }).within(Time.seconds(10));

//        Pattern<JSONObject, JSONObject> pattern1 = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                String lastPage = value.getJSONObject("page").getString("last_page_id");
//                return lastPage == null || lastPage.length() <= 0;
//            }
//        })
//                .times(2)
//                .consecutive()
//                .within(Time.seconds(10));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("TimeOut") {};
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(
                outputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        List<JSONObject> startList = map.get("start");
                        return startList.get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        List<JSONObject> startList = map.get("start");
                        return startList.get(0);
                    }
                });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        unionDS.print(">>>>>>>>>");
        unionDS.map(JSONAware::toJSONString).addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        env.execute();
    }
}
