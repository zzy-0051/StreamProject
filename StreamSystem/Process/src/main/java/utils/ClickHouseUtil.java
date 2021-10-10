package utils;

import bean.TransientSink;
import common.DbConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouseSink(String sql) {

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        Field[] fields = t.getClass().getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];

                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            if (annotation != null) {
                                offset++;
                                continue;
                            }

                            field.setAccessible(true);

                            try {

                                Object value = field.get(t);

                                preparedStatement.setObject(i + 1 - offset, value);

                            } catch (IllegalAccessException e) {

                                e.printStackTrace();

                            }
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(DbConfig.CLICKHOUSE_DRIVER)
                        .withUrl(DbConfig.CLICKHOUSE_URL)
                        .build());

    }
}
