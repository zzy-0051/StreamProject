package data_warehouse.dimJoin;

import com.google.common.cache.*;


import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @Author : zzy
 * @Date : 2021/10/09
 */

public class GuavaCacheCase {
    public static void main(String[] args) {
        Cache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> removalNotification) {
                        System.out.println("[ " + removalNotification.getKey() + " : " + removalNotification.getValue() + " ] is removed ! ");
                    }
                })
                .build();

        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("thread-1");
                try {
                    String value = cache.get("key", new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            System.out.println("load-1");
                            Thread.sleep(1000);
                            return "auto load by Callable";
                        }
                    });
                    System.out.println("thread-1 : " + value);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("thread-2");
                try {
                    String value = cache.get("key", new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            System.out.println("load-2");
                            Thread.sleep(1000);
                            return "auto load by Callable";
                        }
                    });
                    System.out.println("thread-2 : " + value);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        System.out.println(cache.stats());

        LoadingCache<String, String> loadingCache = CacheBuilder.newBuilder().maximumSize(3).build(new CacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
                Thread.sleep(1000);
                System.out.println(key + " is loaded from a cacheLoader !");
                return key + "'s value ";
            }
        });
    }
}
