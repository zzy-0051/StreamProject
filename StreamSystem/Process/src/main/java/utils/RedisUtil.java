package utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class RedisUtil {
    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {

        if (jedisPool == null) {

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 1000);

            System.out.println("Open Connection Pool");
            return jedisPool.getResource();

        } else {
//            System.out.println(" Connection Pool : " + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}
