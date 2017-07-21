package redisclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import serializer.ISerializer;
import serializer.JavaInputSerializer;
import serializer.KryoInputSerializer;

import java.io.Serializable;
import java.util.*;


/**
 * TODO KryoInputSerializer正常序列化后，jedis无法正确存储数据
 */
public class DefaultJedisPoolClient implements Serializable {

    private Logger logger = LoggerFactory.getLogger(DefaultJedisPoolClient.class);
    private static JedisPool pool = null;
    private ISerializer serializer;

//    public DefaultJedisPoolClient(String host, int port, int total, int seconds, boolean testOnBorrow) {
//
//        this(host, port, total, seconds, testOnBorrow,/* new KryoInputSerializer()*/ new JavaInputSerializer());
//    }

    public DefaultJedisPoolClient(String host, int port, int total, int seconds, boolean testOnBorrow, ISerializer serializer) {

        this.serializer = serializer;

        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(total);
            config.setMaxWaitMillis(1000 * seconds);
            config.setTestOnBorrow(testOnBorrow);

            pool = new JedisPool(config, host, port);
        }
    }


    //以Unix时间戳格式设置键的到期时间
    public void expireAt(String key, long unixtime) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.expireAt(serializer.serialize(key), unixtime);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 设定键有效期
     *
     * @param key
     * @param second
     */
    public void expire(String key, int second) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.expire(serializer.serialize(key), second);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 删除键
     *
     * @param key
     */
    public void del(String key) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.del(serializer.serialize(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 将 key 中储存的数字加上指定的增量值
     *
     * @param key
     * @param by
     */
    public void incrby(String key, long by) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.incrBy(serializer.serialize(key), by);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 判断成员元素是否是集合的成员
     *
     * @param key
     * @param mac
     * @return 1 or 0
     */
    public boolean sismember(String key, String mac) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.sismember(serializer.serialize(key), serializer.serialize(mac));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略
     *
     * @param key
     * @param macs
     */
    public void sadd(String key, String... macs) {

        if (macs == null || macs.length == 0) return;

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.sadd(serializer.serialize(key), convertObjectsToBytes(macs));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 用于移除集合中的一个或多个成员元素，不存在的成员元素会被忽略
     *
     * @param key
     * @return 1 or 0
     */
    public void srem(String key, String... macs) {

        if (macs == null || macs.length == 0) return;

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.srem(serializer.serialize(key), convertObjectsToBytes(macs));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 获取存储在集合中的元素的数量
     *
     * @param key
     * @return collection.sizes or 0
     */
    public Long scard(String key) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.scard(serializer.serialize(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 添加所有指定的成员指定的分数存放在键的有序集合
     * 它可以指定多个分/成员对。如果指定的成员已经是有序集合中的一员，分数被更新，
     * 并在合适的位置插入元素，以确保正确的顺序。如果键不存在，一个新的有序集合的指定成员作为唯一的成员创建，
     * 就像如果有序集合是空的。如果该键存在，但不持有有序集合，则返回一个错误
     */
    public void zadd(String key, Map<String, Double> values) {

        if (values == null || values.size() == 0) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.zadd(serializer.serialize(key), convertObjectsToBytesOnlyKey(values));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 从有序集合存储在键删除指定成员。非现有成员被忽略。当键存在，并且不持有有序集合，则会返回错误。
     */
    public void zrem(String key, String... values) {

        if (values == null || values.length == 0) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.zrem(serializer.serialize(key), convertObjectsToBytes(values));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 返回哈希表中，一个或多个给定字段的值
     */
    public <T> List<T> hmget(String key, String[] fileds) {

        if (fileds == null || fileds.length == 0) return null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return convertBytesToListObjects(jedis.hmget(serializer.serialize(key), convertObjectsToBytes(fileds)));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public List<Long> hmgetlong(String key, String[] fileds) {

        if (fileds == null || fileds.length == 0) return null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            List<byte[]> bytes = jedis.hmget(serializer.serialize(key), convertObjectsToBytes(fileds));

            List<Long> res = new ArrayList<>();
            for (int i = 0; i < bytes.size(); i++) {
                if (bytes.get(i) == null) {
                    res.add(null);
                } else {
                    res.add(Long.parseLong(new String(bytes.get(i))));
                }
            }
            return res;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 获取所有字段名保存在键的哈希值
     */
    public <T> Set<T> hkeys(String key) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return convertBytesToSetObjects(jedis.hkeys(serializer.serialize(key)));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void hmset(String key, Map<String, Object> values) {

        if (values == null || values.size() == 0) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hmset(serializer.serialize(key), convertObjectsToBytes(values));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public boolean hexists(String key, String field) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hexists(serializer.serialize(key), serializer.serialize(field));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 给哈希表中的字段赋值
     */
    public void hset(String key, String filed, Object value) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            System.out.println(jedis);
            jedis.hset(serializer.serialize(key), serializer.serialize(filed), serializer.serialize(value));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public long hlen(String key) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hlen(serializer.serialize(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void hincrBy(String key, String filed, long value) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hincrBy(serializer.serialize(key), serializer.serialize(filed), value);
//            jedis.hincrBy(key, filed, value);

        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void hdel(String key, String... fileds) {

        if (fileds == null || fileds.length == 0) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hdel(serializer.serialize(key), convertObjectsToBytes(fileds));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public Object hget(String key, String filed) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return serializer.deserialize(jedis.hget(serializer.serialize(key), serializer.serialize(filed)));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 获取存储在键的散列的所有字段和值
     */
    public <T> Map<String, T> hgetall(String key) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return convertBytesToObjects(jedis.hgetAll(serializer.serialize(key)));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public Object get(String key) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return serializer.deserialize(jedis.get(serializer.serialize(key)));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public Long getLong(String key) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            byte[] bytes = jedis.get(serializer.serialize(key));
            if (bytes != null) {
                return Long.parseLong(new String(jedis.get(serializer.serialize(key))));
            } else {
                return 0l;
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 设置时同时设置过期时间
     * 兼容redis 2.6.12+版本
     *
     * @param key
     * @param obj
     */
    public String set(String key, Object obj, NXXX nxxx, EXPX expx, long time) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.set(serializer.serialize(key), serializer.serialize(obj), nxxx.name().getBytes(), expx.name().getBytes(), time);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 设置时同时设置过期时间
     * 兼容redis 2.6.12+版本
     *
     * @param key
     * @param obj
     */
    public String setEx(String key, Object obj, int time) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.setex(serializer.serialize(key), time, serializer.serialize(obj));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 当存在或不存在时设置
     * 兼容redis 2.6.12+版本
     *
     * @param key
     * @param obj
     */
    public String set(String key, Object obj, NXXX nxxx) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.set(serializer.serialize(key), serializer.serialize(obj), nxxx.name().getBytes());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public void set(String key, Object obj) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.set(serializer.serialize(key), serializer.serialize(obj));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 订阅
     *
     * @param sub
     * @param channels
     */
    public void subscribe(JedisPubSub sub, String... channels) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.subscribe(sub, channels);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 发布
     *
     * @param channel
     * @param message
     */
    public void publish(String channel, String message) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.publish(channel, message);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public Jedis getJedis() {
        return pool.getResource();
    }

    public byte[][] convertObjectsToBytes(Object... objects) {

        byte[][] macBytes = new byte[objects.length][];
        for (int i = 0; i < objects.length; i++) {
            macBytes[i] = serializer.serialize(objects[i]);
        }
        return macBytes;
    }

    public Map<byte[], Double> convertObjectsToBytesOnlyKey(Map<String, Double> objects) {

        Map<byte[], Double> result = new HashMap<>();
        for (String key : objects.keySet()) {
            result.put(serializer.serialize(key), objects.get(key));
        }
        return result;
    }

    public Map<byte[], byte[]> convertObjectsToBytes(Map<String, Object> objects) {

        Map<byte[], byte[]> result = new HashMap<>();
        for (String key : objects.keySet()) {
            result.put(serializer.serialize(key), serializer.serialize(objects.get(key)));
        }
        return result;
    }

    public <T> Map<String, T> convertBytesToObjects(Map<byte[], byte[]> bytes) {

        Map<String, T> result = new HashMap<>();
        for (byte[] key : bytes.keySet()) {
            result.put((String) serializer.deserialize(key), (T) serializer.deserialize(bytes.get(key)));
        }
        return result;
    }

    public <T> List<T> convertBytesToListObjects(Collection<byte[]> bytes) {

        List<T> result = new ArrayList<>();
        for (byte[] objByte : bytes) {
            result.add((T) serializer.deserialize(objByte));
        }
        return result;
    }

    public <T> Set<T> convertBytesToSetObjects(Collection<byte[]> bytes) {

        Set<T> result = new HashSet<>();
        for (byte[] objByte : bytes) {
            result.add((T) serializer.deserialize(objByte));
        }
        return result;
    }

    /**
     * NX ：只在键不存在时，才对键进行设置操作。 SET key value NX 效果等同于 SETNX key value 。
     * XX ：只在键已经存在时，才对键进行设置操作。
     */
    public enum NXXX {
        NX, XX
    }

    /**
     * EX second ：设置键的过期时间为 second 秒。 SET key value EX second 效果等同于 SETEX key second value 。
     * PX millisecond ：设置键的过期时间为 millisecond 毫秒。 SET key value PX millisecond 效果等同于 PSETEX key
     */
    public enum EXPX {
        EX, PX
    }
}
