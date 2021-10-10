package core.service;


import bean.OrderWide;
import bean.PaymentWide;
import bean.ProductStats;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.AppConstant;
import core.func.async_io.AsyncDimFunc;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.ClickHouseUtil;
import utils.DateTimeUtil;
import utils.KafkaUtil;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStat {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //  Kafka_partition
        env.setParallelism(1);

    //  env.enableCheckpointing(5000L);
    //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = KafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = KafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = KafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = KafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = KafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(value);

                //{"during_time":5289,"item":"3","item_type":"sku_id","last_page_id":"good_list","page_id":"good_detail","source_type":"promotion"}
                Long ts = jsonObject.getLong("ts");
                System.out.println("Behaviour ：" + ts);
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(line -> {

            JSONObject jsonObject = JSONObject.parseObject(line);

            System.out.println("Business ：" + DateTimeUtil.toTs(jsonObject.getString("create_time")));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {

            JSONObject jsonObject = JSONObject.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {

            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);

            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(orderWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_amount(orderWide.getTotal_amount())
                    .order_sku_num(orderWide.getSku_num())
                    .orderIdSet(orderIdSet)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(line -> {

            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);

            HashSet<Long> payOrderIdSet = new HashSet<>();
            payOrderIdSet.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getTotal_amount())
                    .paidOrderIdSet(payOrderIdSet)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {

            JSONObject jsonObject = JSONObject.parseObject(line);

            HashSet<Long> refundOrderIdSet = new HashSet<>();
            refundOrderIdSet.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refundOrderIdSet(refundOrderIdSet)
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {

            JSONObject jsonObject = JSONObject.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            Long good = 0L;
            if (AppConstant.APPRAISE_GOOD.equals(appraise)) {
                good = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(good)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithCartDS,
                productStatsWithCommentDS,
                productStatsWithFavorDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS
        );

        KeyedStream<ProductStats, Long> keyedStream = unionDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }))
                .keyBy(ProductStats::getSku_id);

        SingleOutputStreamOperator<ProductStats> result = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        ProductStats productStats = input.iterator().next();

                        String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                        String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                        productStats.setStt(stt);
                        productStats.setEdt(edt);

                        out.collect(productStats);
                    }
                });

        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(result,
                new AsyncDimFunc<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        BigDecimal price = dimInfo.getBigDecimal("PRICE");
                        String sku_name = dimInfo.getString("SKU_NAME");

                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        productStats.setSku_price(price);
                        productStats.setSku_name(sku_name);
                        productStats.setSpu_id(spu_id);
                        productStats.setTm_id(tm_id);
                        productStats.setCategory3_id(category3_id);
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new AsyncDimFunc<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        String spu_name = dimInfo.getString("SPU_NAME");

                        productStats.setSpu_name(spu_name);
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new AsyncDimFunc<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        String tm_name = dimInfo.getString("TM_NAME");

                        productStats.setTm_name(tm_name);
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithTmDS,
                new AsyncDimFunc<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        productStats.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        productStatsWithCategory3DS.print(">>>>>>>>>>");
        productStatsWithCategory3DS.addSink(ClickHouseUtil.getClickHouseSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
