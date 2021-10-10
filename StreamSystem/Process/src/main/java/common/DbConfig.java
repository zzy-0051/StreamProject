package common;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class DbConfig {
    public static final String HBASE_SCHEMA = "STREAM_SYSTEM";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static final String PHOENIX_SERVER = "jdbc:phoenix:zmaster,znode1,znode2:2181";

    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://zmaster:8123/stream_system";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
