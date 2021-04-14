package utils;

import redis.clients.jedis.Jedis;

public class RedisUtils {
    private static final Jedis jedis = new Jedis("localhost");

    public static void set(String key, String value) {
        jedis.set(key, value);
    }

    public static String get(String key) {
        return jedis.get(key);
    }
}
