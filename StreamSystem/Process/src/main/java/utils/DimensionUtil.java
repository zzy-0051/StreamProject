package utils;

import com.alibaba.fastjson.JSONObject;
import common.DbConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class DimensionUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            jedis.expire(redisKey, 60 * 60 * 24);
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }

        String querySql = "select * from " + DbConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";

        List<JSONObject> queryList = JdbcUtil.query(connection, querySql, JSONObject.class, false);

        JSONObject jsonObject = queryList.get(0);
        jedis.set(redisKey, jsonObject.toJSONString());

        jedis.expire(redisKey, 60 * 60 * 24);
        jedis.close();

        return jsonObject;

    }

    public static void delRedisDimInfo(String tableName, String id) {

        String redisKey = "DIM:" + tableName + ":" + id;

        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);

        jedis.close();

    }
}
