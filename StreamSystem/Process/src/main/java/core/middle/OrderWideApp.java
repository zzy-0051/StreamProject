package core.middle;

import app.func.AsyncDimFunction;
import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.KafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

    //  env.enableCheckpointing(5000L);
    //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/cdc/ck"));

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

       //       Order_Info_KafkaDS
        SingleOutputStreamOperator<OrderInfo> orderInfoKafkaDS = env.addSource(
                KafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(data -> {
                    OrderInfo orderInfo = JSONObject.parseObject(data, OrderInfo.class);

                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");

                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderInfo;
                });

        //      Order_Detail_KafkaDS
        SingleOutputStreamOperator<OrderDetail> orderDetailKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(data -> {
                    OrderDetail orderDetail = JSONObject.parseObject(data, OrderDetail.class);

                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderDetail;
                });

        //      Order_Info_DS
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //      Order_Detail_DS
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = (SingleOutputStreamOperator<OrderWide>) orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        orderWideDS.print("OrderWide>>>>>>>>>>>");


        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);

                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long ts = sdf.parse(birthday).getTime();

                        Long age = (System.currentTimeMillis() - ts) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age(new Integer(age.toString()));
                    }
                },
                60,
                TimeUnit.SECONDS);

//        orderWideWithUserDS.print("User>>>>>>>>>>");

        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String code2 = dimInfo.getString("ISO_3166_2");

                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(code2);

                    }
                }, 60,
                TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        orderWide.setSpu_id(spu_id);
                        orderWide.setTm_id(tm_id);
                        orderWide.setCategory3_id(category3_id);

                    }
                }, 60,
                TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        String spu_name = dimInfo.getString("SPU_NAME");

                        orderWide.setSpu_name(spu_name);
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("Connect All Dimension>>>");
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(orderWideSinkTopic));

        env.execute();

    }
}
