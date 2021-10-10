package core.detail;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.KafkaUtil;

/**
 * @Author : zzy
 * @Date : 2021/09/27
 */

public class LogProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String sourceTopic = "ods_base_log";
        String groupId = "ods_base_log_group";

        OutputTag<String> dirtyData = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            ctx.output(dirtyData, value);
                        }
                    }
                });

        jsonObjDS.getSideOutput(dirtyData).print("DirtyData >>> ");

        //  keyBy -> mid
        KeyedStream<JSONObject, String> midKeyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = midKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is-new", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                String isNew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {

                    String state = valueState.value();

                    if (state != null) {

                        value.getJSONObject("common").put("is_new", "0");

                    } else {

                        valueState.update("0");

                    }
                }
                return value;
            }
        });

        //  Output
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                //  "start"
                String start = value.getString("start");

                if (start != null && start.length() > 0) {

                    ctx.output(startTag, value.toJSONString());

                } else {

                    out.collect(value.toJSONString());

                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {

                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);

                            String pageId = value.getJSONObject("page").getString("page_id");
                            display.put("page_id", pageId);

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("Page >>> ");
        startDS.print("Start >>> ");
        displayDS.print("Display >>> ");

        pageDS.addSink(KafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(KafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(KafkaUtil.getKafkaProducer("dwd_display_log"));

        env.execute();

    }
}
