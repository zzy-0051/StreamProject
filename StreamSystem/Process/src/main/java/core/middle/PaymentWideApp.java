package core.middle;

import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.KafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  env.enableCheckpointing(5000L);
        //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/cdc/ck"));

        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        SingleOutputStreamOperator<PaymentInfo> paymentKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, PaymentInfo.class));

        SingleOutputStreamOperator<OrderWide> orderWideKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, OrderWide.class));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        paymentWideDS.print(">>>>>>>");
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        env.execute();
    }
}
