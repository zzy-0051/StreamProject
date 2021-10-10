package core.func;

import com.alibaba.fastjson.JSONObject;
import common.DbConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import utils.DimensionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class DimensionSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(DbConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(DbConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, SinkFunction.Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {

            String tableName = value.getString("sinkTable");
            JSONObject data = value.getJSONObject("data");
            String upsertSql = genUpsertSql(tableName, data);
            System.out.println(upsertSql);

            preparedStatement = connection.prepareStatement(upsertSql);

            if (value.getString("type").equals("update")) {
                DimensionUtil.delRedisDimInfo(tableName.toUpperCase(), data.getString("id"));
            }
            preparedStatement.execute();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSql(String tableName, JSONObject data) {

        StringBuilder sql = new StringBuilder("upsert into ")
                .append(DbConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append("(");

        Set<String> keySet = data.keySet();
        sql
                .append(StringUtils.join(keySet, ","))
                .append(") values ('");

        Collection<Object> values = data.values();
        sql.append(StringUtils.join(values, "','"))
                .append("')");

        return sql.toString();
    }
}
