package core.service;


import common.AppConstant;
import core.func.SplitKeywordUDTF;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.ClickHouseUtil;
import utils.KafkaUtil;

public class KeywordStats {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    //  env.enableCheckpointing(5000L);
    //  env.getCheckpointConfig().setCheckpointTimeout(5000L);
    //  env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnv.executeSql("create table page_log(" +
                "   `common` MAP<STRING,STRING>, " +
                "   `page` MAP<STRING,STRING>, " +
                "   `ts` BIGINT, " +
                "   `row_time` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "   WATERMARK FOR row_time AS row_time " +
                ") with" + KafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId));

        Table full_word_Table = tableEnv.sqlQuery("" +
                "select " +
                "   page['item'] full_word, " +
                "   ts, " +
                "   row_time " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null");

        tableEnv.createTemporarySystemFunction("split_word", SplitKeywordUDTF.class);
        Table splitWordTable = tableEnv.sqlQuery("SELECT word,ts,row_time " +
                "FROM " + full_word_Table + ", LATERAL TABLE(split_word(full_word))");

        Table result = tableEnv.sqlQuery("select " +
                "'" + AppConstant.KEYWORD_SEARCH + "' source," +
                "   DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   word keyword, " +
                "   count(*) ct, " +
                "   max(ts) ts " +
                "from " + splitWordTable + " " +
                "group by TUMBLE(row_time, INTERVAL '10' SECOND),word");

        DataStream<bean.KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(result, bean.KeywordStats.class);

        keywordStatsDataStream.print(">>>>>>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into keyword_stats_210108(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        env.execute();

    }
}
