import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author : zzy
 * @Date : 2021/09/27
 */

public class CaptureWithSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String ddl = ""
                + "CREATE TABLE mysql_tb ( "
                + "id INT, "
                + "name STRING, "
                + "address STRING "
                + ") WITH ( "
                + "'connector' = 'mysql-cdc', "
                + "'hostname' = 'znode2', "
                + "'port' = '3306', "
                + "'username' = 'root', "
                + "'password' = 'ZengZhenyu0622', "
                + "'database-name' = 'stream_test', "
                + "'table-name' = 'user', "
                +"'scan.incremental.snapshot.enabled' = 'false' "
                + ")";
        tEnv.executeSql(ddl);

        String query = "SELECT * FROM mysql_tb";
        Table table = tEnv.sqlQuery(query);

        tEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}
