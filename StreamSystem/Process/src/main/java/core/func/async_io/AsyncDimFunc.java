package core.func.async_io;

import com.alibaba.fastjson.JSONObject;
import common.DbConfig;
import core.func.async_io.util.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.DimensionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author : zzy
 * @Date : 2021/09/30
 */

public abstract class AsyncDimFunc<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunc<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public AsyncDimFunc(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(DbConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(DbConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        threadPoolExecutor.submit(
                new Runnable() {
                    @SneakyThrows
                    @Override
                    public void run() {
                        String key = getKey(input);
                        JSONObject dimInfo = DimensionUtil.getDimInfo(connection, tableName, key);
                        if (dimInfo != null) {
                            join(input, dimInfo);
                        }
                        resultFuture.complete(Collections.singletonList(input));
                    }
                }
        );
    }
}

