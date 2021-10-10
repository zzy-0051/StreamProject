package utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    public static ThreadPoolExecutor pool;

    private ThreadPoolUtil() {}

    public static ThreadPoolExecutor getInstance() {

        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {


//    corePoolSize , maximumPoolSize , keepAliveTime , unit:keepAliveTime , workQueue
                    pool = new ThreadPoolExecutor(
                            4,
                            20,
                            300L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}
