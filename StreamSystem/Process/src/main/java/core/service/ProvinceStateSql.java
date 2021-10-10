package core.service;

import bean.ProvinceStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.ClickHouseUtil;
import utils.KafkaUtil;

public class ProvinceStateSql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    //  env.enableCheckpointing(5000L);
    //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        String groupId = "province_stats";
        String topic = "dwm_order_wide";
        tableEnv.executeSql(
                "CREATE TABLE order_wide (  " +
                "  `province_id` BIGINT,  " +
                "  `province_name` STRING,  " +
                "  `province_area_code` STRING,  " +
                "  `province_iso_code` STRING,  " +
                "  `province_3166_2_code` STRING,  " +
                "  `order_id` BIGINT,  " +
                "  `total_amount` DECIMAL,  " +
                "  `create_time` STRING,  " +
                "  `rowtime` as TO_TIMESTAMP(create_time),  " +
                "  WATERMARK FOR rowtime AS rowtime  " +
                ") with" + KafkaUtil.getKafkaDDL(topic, groupId));

        Table table = tableEnv.sqlQuery(
                "select " +
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   province_id,  " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   count(distinct order_id) order_count, " +
                "   sum(total_amount) order_amount, " +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND), " +
                "   province_id," +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code");

        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        provinceStatsDataStream.print(">>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_210108 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();

    }
}
