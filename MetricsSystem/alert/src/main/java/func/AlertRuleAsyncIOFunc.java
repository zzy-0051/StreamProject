package func;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import model.MetricEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

@Slf4j
public class AlertRuleAsyncIOFunc extends RichAsyncFunction<MetricEvent, MetricEvent> {

    PreparedStatement preparedStatement;
    private Connection connection;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = getConnection();
        String query = "select * from alert_rule where name = ?;";
        if (connection != null) {
            preparedStatement = this.connection.prepareStatement(query);
        }
    }

    private static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://zmaster:3306/metrics_system?useUnicode=true&characterEncoding=UTF-8";
            String user = "root";
            String password = "root";
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            log.error(" --- mysql get connection has exception , msg = {}", e.getMessage());
        }
        return connection;
    }


    @SneakyThrows
    @Override
    public void asyncInvoke(MetricEvent metricEvent, ResultFuture<MetricEvent> resultFuture) {
        preparedStatement.setString(1, metricEvent.getName());
        ResultSet resultSet = preparedStatement.executeQuery();
        Map<String, Object> fields = metricEvent.getFields();
        if (resultSet.next()) {
            String thresholds = resultSet.getString("thresholds");
            String measurement = resultSet.getString("measurement");
            if (fields.get(measurement) != null && (double)fields.get(measurement) > Double.parseDouble(thresholds)) {
                List<MetricEvent> eventList = new ArrayList<>();
                eventList.add(metricEvent);
                resultFuture.complete(Collections.singletonList(metricEvent));
            }
        }
    }

    @Override
    public void timeout(MetricEvent metricEvent, ResultFuture<MetricEvent> resultFuture) {
        log.info(" === timeout === {} ", metricEvent);
    }
}
