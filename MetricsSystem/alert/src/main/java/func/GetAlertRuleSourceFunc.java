package func;

import lombok.extern.slf4j.Slf4j;
import model.AlertRule;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

@Slf4j
public class GetAlertRuleSourceFunc extends RichSourceFunction<List<AlertRule>> {

    private PreparedStatement preparedStatement;
    private Connection connection;
    private volatile boolean isRunning;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = getConnection();
        String query = "select * from alert_rule";
        if (connection != null) {
            preparedStatement = this.connection.prepareStatement(query);
        }
    }

    private static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.jdbc.mysql.Driver");
            String url = "jdbc:mysql://zmaster:3306/metric_system?useUnicode=true&characterEncoding=UTF-8";
            String user = "root";
            String password = "root";
            connection = DriverManager.getConnection(url,user,password);
        } catch (Exception e) {
            log.error(" --- mysql get connection has exception , msg = {}", e.getMessage());
        }
        return connection;
    }

    @Override
    public void run(SourceContext<List<AlertRule>> ctx) throws Exception {
        List<AlertRule> ruleList = new ArrayList<>();
        while (isRunning) {
            ResultSet resultSet = preparedStatement.executeQuery();
            AlertRule alertRule = new AlertRule().builder()
                    .id(resultSet.getInt("id"))
                    .name(resultSet.getString("name"))
                    .measurement(resultSet.getString("measurement"))
                    .thresholds(resultSet.getString("thresholds"))
                    .build();
            ruleList.add(alertRule);
        }
        log.info(" === select alarm notify from mysql, size = {}, map = {}", ruleList.size(), ruleList);

        ctx.collect(ruleList);
        ruleList.clear();
        Thread.sleep(1000 * 60);
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (Exception e) {
            log.error("runException:{}", e);
        }
        isRunning = false;
    }
}
