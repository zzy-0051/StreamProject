package core.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ClickHouseUtil;
import utils.DateTimeUtil;
import utils.KafkaUtil;

import java.time.Duration;
import java.util.Date;

public class VisitorStats {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  Kafka_partitions
        env.setParallelism(1);

    //  env.enableCheckpointing(5000L);
    //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/cdc/ck"));

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";
        SingleOutputStreamOperator<JSONObject> uvKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId))
                .map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> ujKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId))
                .map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> pageKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId))
                .map(JSONObject::parseObject);

        SingleOutputStreamOperator<bean.VisitorStats> visitorStatsUvDS = uvKafkaDS.map(json -> {

            JSONObject common = json.getJSONObject("common");

            return new bean.VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    json.getLong("ts")
            );
        });

        SingleOutputStreamOperator<bean.VisitorStats> visitorStatsUjDS = ujKafkaDS.map(json -> {

            JSONObject common = json.getJSONObject("common");

            return new bean.VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    json.getLong("ts")
            );
        });

        SingleOutputStreamOperator<bean.VisitorStats> visitorStatsPageDS = pageKafkaDS.map(json -> {

            JSONObject common = json.getJSONObject("common");

            JSONObject page = json.getJSONObject("page");
            Long durTime = page.getLong("during_time");

            long sv = 0L;
            String last_page_id = page.getString("last_page_id");
            if (last_page_id == null) {
                sv = 1L;
            }

            return new bean.VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    durTime,
                    json.getLong("ts")
            );
        });

        DataStream<bean.VisitorStats> unionDS = visitorStatsUvDS.union(visitorStatsUjDS, visitorStatsPageDS);

        SingleOutputStreamOperator<bean.VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<bean.VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<bean.VisitorStats>() {
                    @Override
                    public long extractTimestamp(bean.VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        KeyedStream<bean.VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS.keyBy(new KeySelector<bean.VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(bean.VisitorStats value) throws Exception {
                return new Tuple4<>(
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new(),
                        value.getVc()
                );
            }
        });
        WindowedStream<bean.VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<bean.VisitorStats> result = windowedStream.reduce(new ReduceFunction<bean.VisitorStats>() {
            @Override
            public bean.VisitorStats reduce(bean.VisitorStats value1, bean.VisitorStats value2) throws Exception {
                return new bean.VisitorStats(
                        "",
                        "",
                        value1.getVc(),
                        value1.getCh(),
                        value1.getAr(),
                        value1.getIs_new(),
                        value1.getUv_ct() + value2.getUv_ct(),
                        value1.getPv_ct() + value2.getPv_ct(),
                        value1.getSv_ct() + value2.getSv_ct(),
                        value1.getUj_ct() + value2.getUj_ct(),
                        value1.getDur_sum() + value2.getDur_sum(),
                        value2.getTs()
                );
            }
        }, new WindowFunction<bean.VisitorStats, bean.VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<bean.VisitorStats> input, Collector<bean.VisitorStats> out) throws Exception {

                long start = window.getStart();
                long end = window.getEnd();

                bean.VisitorStats visitorStats = input.iterator().next();

                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(visitorStats);
            }
        });

        result.print(">>>>>>>>>>>");
        result.addSink(ClickHouseUtil.getClickHouseSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
