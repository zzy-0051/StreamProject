package core.func;

import bean.ConfigTableBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.DbConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class ConfigTableFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, ConfigTableBean> mapStateDescriptor;
    private Connection connection;

    public ConfigTableFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, ConfigTableBean> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(DbConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(DbConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, ConfigTableBean> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
        ConfigTableBean configTableBean = broadcastState.get(key);
        if (configTableBean != null) {
            filterColumn(value.getJSONObject("data"),configTableBean.getSinkColumns());
            value.put("sinkTable",configTableBean.getSinkTable());
            if (configTableBean.getSinkType().equals(ConfigTableBean.SINK_TYPE_HBASE)) {
                ctx.output(outputTag,value);
            } else if (configTableBean.getSinkType().equals(ConfigTableBean.SINK_TYPE_KAFKA)) {
                out.collect(value);
            }
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columnsArr = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columnsArr);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnsList.contains(next.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        ConfigTableBean configTableBean = JSON.parseObject(jsonObject.getString("data"), ConfigTableBean.class);
        if (configTableBean.getSinkTable().equals(ConfigTableBean.SINK_TYPE_HBASE)) {
            checkTable(
                    configTableBean.getSinkTable(),
                    configTableBean.getSinkColumns(),
                    configTableBean.getSinkPk(),
                    configTableBean.getSinkExtend()
            );
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder createSql = new StringBuilder("create table if not exists ");
            createSql.append(DbConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    createSql.append(column).append(" varchar primary key ");
                } else {
                    createSql.append(column).append(" varchar ");
                }

                if (i < columns.length - 1) {
                    createSql.append(",");
                }
            }

            createSql.append(")").append(sinkExtend);

            System.out.println(createSql.toString());

            preparedStatement = connection.prepareStatement(createSql.toString());

            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("Phoenix Create " + sinkTable + " Failed.");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
