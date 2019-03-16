import com.alibaba.fastjson.JSON;
import redis.clients.jedis.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Redis缓存的帮助类
 * Key统一定义： dw:{sub system}:{module}:cacheType:key，如
 * dw:dfs:web:fileId_fileUid:2   表示在dw:dfs:web子系统下，存储key的类型是fileId, 存储value的类型为fileUid, 存储的key值为2
 *
 * @author LJ
 * <p>
 * 修改redis读取配置文件方式，从spring cloud config配置中心读取配置文件
 * 以@value方式注入，添加refreshscope，支持动态更新
 * @Modified by zzaki on 2018-06-07
 */
public class RedisCache {
    // 默认的锁过期时间
    private static final int LOCK_DEFAULT_EXPIRE_TIME = 3;
    //缓存Key的默认前缀，如dw:dfs:web, dw:ris:web等
    private static String DEFAULT_KEY_PREFIX = "";
    //默认的过期时间（秒），小于等于0，则表示不过期
    private static int DEFAULT_EXPIRE_TIME = 0;
    //地址，端口，密码等信息
    private static String ADDR = null;
    private static int PORT = 6379;
    private static String AUTH = null;

    //连接池相关默认值
    private static int MAX_ACTIVE = 300;
    private static int MAX_IDLE = 200;
    private static int MAX_WAIT = 10000;
    private static int TIMEOUT = 10000;

    private static boolean TEST_ON_BORROW = true;
    private static JedisPool jedisPool = null;
    private static Jedis jedis = null;

    public static String getDefaultKeyPrefix() {
        return DEFAULT_KEY_PREFIX;
    }

    public void setDefaultKeyPrefix(String redisKeyPrefix) {
        DEFAULT_KEY_PREFIX = redisKeyPrefix;
    }

    private synchronized static void init() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(MAX_IDLE);
        config.setMaxWaitMillis(MAX_WAIT);
        config.setTestOnBorrow(TEST_ON_BORROW);
        config.setMaxTotal(MAX_ACTIVE);
        jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH);
    }


    public static byte[] serialize(Object obj){
        ObjectOutputStream oos = null;
        ByteArrayOutputStream byteOut = null;
        try{
            byteOut = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(byteOut);
            oos.writeObject(obj);
            byte[] bytes = byteOut.toByteArray();
            return bytes;
        }catch (Exception e) {
            return null;
        }
    }

    public static Object unSerialize(byte[] bytes){
        ByteArrayInputStream in = null;
        try{
            in = new ByteArrayInputStream(bytes);
            ObjectInputStream objIn = new ObjectInputStream(in);
            return objIn.readObject();
        }catch (Exception e) {
            return null;
        }
    }

    /**
     * 设置单个值，
     *
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @param value:     存储数据
     * @return 成功 返回OK 失败返回 FAIL
     */
    public static String set(String cacheType, String key, String value) {
        return set(generateFullKey(cacheType, key), value, DEFAULT_EXPIRE_TIME);
    }

    /**
     * @param cacheType:     缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:           key值，如2
     * @param value:         存储数据
     * @param expireSeconds: 过期时间
     * @return 成功 返回OK 失败返回 FAIL
     */
    public static String set(String cacheType, String key, String value, int expireSeconds) {
        return set(generateFullKey(cacheType, key), value, expireSeconds);
    }

    /**
     * @param prefix:    前缀，表示项目模块，如dw:dfs:web
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @param value:     存储数据
     * @return 成功 返回OK 失败返回 FAIL
     */
    public static String set(String prefix, String cacheType, String key, String value) {
        return set(generateFullKey(prefix, cacheType, key), value, DEFAULT_EXPIRE_TIME);
    }

    /**
     * @param prefix:       前缀，表示项目模块，如dw:dfs:web
     * @param cacheType:    缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:          key值，如2
     * @param value:        存储数据
     * @param expireSeconds
     * @return 成功 返回OK 失败返回 FAIL
     */
    public static String set(String prefix, String cacheType, String key, String value, int expireSeconds) {
        return set(generateFullKey(prefix, cacheType, key), value, expireSeconds);
    }

    /**
     * @param prefix:       前缀，表示项目模块，如dw:dfs:web
     * @param cacheType:    缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:          key值，如2
     * @param value:        存储数据
     * @param expireSeconds
     * @return 成功 返回OK 失败返回 FAIL
     */
    public static String setByExpireAt(String prefix, String cacheType, String key, String value, int expireSeconds) {
        return setByExpireAt(generateFullKey(prefix, cacheType, key), value, expireSeconds);
    }

    /**
     * 设置单个值，并设置超时时间
     *
     * @param fullKey:      根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @param value:        存储数据
     * @param expireSeconds 超时时间（秒）
     * @return 成功 返回OK 失败返回 FAIL
     */
    private static String set(String fullKey, String value, int expireSeconds) {
        String result = "FAIL";
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                result = jedis.set(fullKey, value);
                if (expireSeconds > 0) {
                    jedis.expire(fullKey, expireSeconds);
                }
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 设置单个值，并设置超时时间
     *
     * @param fullKey:      根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @param value:        存储数据
     * @param expireSeconds 超时时间（秒）
     * @return 成功 返回OK 失败返回 FAIL
     */
    private static String setByExpireAt(String fullKey, String value, int expireSeconds) {
        String result = "FAIL";
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                result = jedis.set(fullKey, value);
                if (expireSeconds > 0) {
                    jedis.expireAt(fullKey, expireSeconds);
                }
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 设置Object
     *
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @param value:     存储数据
     * @return 成功 返回OK 失败返回 FAIL
     */
    public static String setObj(String cacheType, String key, Object value) {
        return setObj(generateFullKey(cacheType, key), value, DEFAULT_EXPIRE_TIME);
    }

    /**
     * 设置Object
     *
     * @param cacheType:    缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:          key值，如2
     * @param value:        存储数据
     * @param expireSeconds
     * @return
     */
    public static String setObj(String cacheType, String key, Object value, int expireSeconds) {
        return setObj(generateFullKey(cacheType, key), value, expireSeconds);
    }

    /**
     * @param prefix:       前缀，表示项目模块，如dw:dfs:web
     * @param cacheType:    缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:          key值，如2
     * @param value:        存储数据
     * @param expireSeconds
     * @return
     */
    public static String setObj(String prefix, String cacheType, String key, Object value, int expireSeconds) {
        return setObj(generateFullKey(prefix, cacheType, key), value, expireSeconds);
    }

    /**
     * 设置Object
     * @param cacheType:    缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:          key值，如2
     * @param value:        存储数据
     * @param expireSeconds 失效时间，到某个时间戳失效
     * @return java.lang.String
     * @author lanxuewei 2019/2/16 18:41
     */
    public static String setObjByExpireAt(String cacheType, String key, Object value, long expireSeconds) {
        return setObjByExpireAt(generateFullKey(cacheType, key), value, expireSeconds);
    }

    /**
     * 设置Object，并设置超时时间
     *
     * @param fullKey:   根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @param value:     存储数据
     * @param expireTime 超时时间（秒）
     * @return 成功 返回OK 失败返回 FAIL
     */
    private static String setObj(String fullKey, Object value, int expireTime) {
        String result = "FAIL";
        Jedis jedis = null;
        try {
            jedis = getJedis();
            byte[] bs = serialize(value);
            if (bs != null) {
                result = jedis.set(fullKey.getBytes(), bs);
                if (expireTime > 0) {
                    jedis.expire(fullKey, expireTime);
                }
            }
        } catch (Exception e) {
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * description: 设置Object，并设置超时时间
     * @param fullKey: 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @param value: 存储数据
     * @param expireTime 失效时间 到某个时间戳失效
     * @return 成功 返回OK 失败返回 FAIL
     * @author lanxuewei 2019/2/16 18:38
     */
    private static String setObjByExpireAt(String fullKey, Object value, long expireTime) {
        String result = "FAIL";
        Jedis jedis = null;
        try {
            jedis = getJedis();
            byte[] bs = serialize(value);
            if (bs!=null) {
                result = jedis.set(fullKey.getBytes(), bs);
                if (expireTime > 0) {
                    jedis.expireAt(fullKey, expireTime);
                }
            }
        } catch (Exception e) {
        } finally {
            if (jedis!=null) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 获取缓存的值
     *
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static String get(String cacheType, String key) {
        return get(generateFullKey(cacheType, key));
    }

    /**
     * 获取缓存的值
     *
     * @param prefix:    前缀，表示项目模块，如dw:dfs:web
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static String get(String prefix, String cacheType, String key) {
        return get(generateFullKey(prefix, cacheType, key));
    }

    /**
     * 获取缓存的值
     *
     * @param fullKey: 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @return
     */
    private static String get(String fullKey) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                return jedis.get(fullKey);
            }
        } catch (Exception e) {
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static Object getObj(String cacheType, String key) {
        return getObj(generateFullKey(cacheType, key));
    }

    /**
     * @param prefix:   前缀，表示项目模块，如dw:dfs:web
     * @param cacheType
     * @param key
     * @return
     */
    public static Object getObj(String prefix, String cacheType, String key) {
        return getObj(generateFullKey(prefix, cacheType, key));
    }

    /**
     * 获取Object
     *
     * @param fullKey: 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @return 成功返回value 失败返回null
     */
    private static Object getObj(String fullKey) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                byte[] value = jedis.get(fullKey.getBytes());
                if (value != null) {
                    return unSerialize(value);
                }
            }
        } catch (Exception e) {
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static boolean del(String cacheType, String key) {
        return del(generateFullKey(cacheType, key));
    }

    /**
     * @param prefix:    前缀，表示项目模块，如dw:dfs:web
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static boolean del(String prefix, String cacheType, String key) {
        return del(generateFullKey(prefix, cacheType, key));
    }

    /**
     * 删除redis中数据
     *
     * @param fullKey: 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @return
     */
    private static boolean del(String fullKey) {
        Boolean result = Boolean.FALSE;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                jedis.del(fullKey);
                result = Boolean.TRUE;
            }
        } catch (Exception e) {
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 往一个key上追加缓存
     *
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @param value:     存储数据
     * @return
     */
    public static Long append(String cacheType, String key, String value) {
        return append(generateFullKey(cacheType, key), value);
    }

    /**
     * 往一个key上追加缓存
     *
     * @param prefix:    前缀，表示项目模块，如dw:dfs:web
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @param value:     存储数据
     * @return
     */
    public static Long append(String prefix, String cacheType, String key, String value) {
        return append(generateFullKey(prefix, cacheType, key), value);
    }

    /**
     * 追加
     *
     * @param fullKey: 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @param value
     * @return
     */
    private static Long append(String fullKey, String value) {
        Long result = Long.valueOf(0);
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                result = jedis.append(fullKey, value);
            }
        } catch (Exception e) {
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 判断是否存在缓存
     *
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static Boolean exists(String cacheType, String key) {
        return exists(generateFullKey(cacheType, key));
    }

    /**
     * @param prefix:    前缀，表示项目模块，如dw:dfs:web
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return
     */
    public static Boolean exists(String prefix, String cacheType, String key) {
        return exists(generateFullKey(prefix, cacheType, key));
    }

    /**
     * 检测key是否存在
     *
     * @param fullKey: 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     * @return
     */
    private static Boolean exists(String fullKey) {
        Boolean result = Boolean.FALSE;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                result = jedis.exists(fullKey);
            }
        } catch (Exception e) {
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 订阅频道
     *
     * @param subscriber  消息订阅者
     * @param channelName 订阅的频道名称
     */
    public static void subscribe(JedisPubSub subscriber, String channelName) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                jedis.subscribe(subscriber, channelName);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
    }

    /**
     * 向频道发送消息
     *
     * @param channel 频道名称
     * @param msg     需要发送的消息
     */
    public static Long publish(String channel, String msg) {
        Long result = 0L;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.publish(channel, msg);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 获取hash值
     *
     * @param prefix    自定义业务前缀
     * @param cacheType 缓存类型
     * @param key       hash的键
     * @param field     hash的域
     */
    public static String hget(String prefix, String cacheType, String key, String field) {
        return hget(generateFullKey(prefix, cacheType, key), field);
    }

    /**
     * 获取hash值
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     * @param field     hash的域
     */
    public static String hget(String cacheType, String key, String field) {
        return hget(generateFullKey(cacheType, key), field);
    }

    /**
     * 获取hash值
     *
     * @param key
     * @param field
     */
    private static String hget(String key, String field) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hget(key, field);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 设置Hash值
     *
     * @param prefix    自定义业务前缀
     * @param cacheType  缓存类型
     * @param key        hash的键
     * @param field      hash的域
     * @param value      需要设置的field的值
     * @param expireTime 过期时间
     */
    public static Long hset(String prefix, String cacheType, String key, String field, String value, int expireTime) {
        return hset(generateFullKey(prefix, cacheType, key), field, value, expireTime);
    }

    /**
     * 设置Hash值
     *
     * @param cacheType  缓存类型
     * @param key        hash的键
     * @param field      hash的域
     * @param value      需要设置的field的值
     * @param expireTime 过期时间
     */
    public static Long hset(String cacheType, String key, String field, String value, int expireTime) {
        return hset(generateFullKey(cacheType, key), field, value, expireTime);
    }

    /**
     * 设置Hash值
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     * @param field     hash的域
     * @param value     需要设置的field的值
     */
    public static Long hset(String cacheType, String key, String field, String value) {
        return hset(generateFullKey(cacheType, key), field, value, DEFAULT_EXPIRE_TIME);
    }

    /**
     * 设置hash值
     *
     * @param key        hash的key
     * @param field      hash的field
     * @param value      hash field对应的value
     * @param expireTime hash的过期时间
     */
    private static Long hset(String key, String field, String value, int expireTime) {
        Long result = -1L;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hset(key, field, value);
                if (expireTime > 0) {
                    jedis.expire(key, expireTime);
                }
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 值不存在则设置
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     * @param field     hash的域
     * @param value     需要设置的field的值
     */
    public static Long hsetnx(String cacheType, String key, String field, String value) {
        return hsetnx(generateFullKey(cacheType, key), field, value, DEFAULT_EXPIRE_TIME);
    }

    /**
     * 值不存在则设置
     *
     * @param key        hash的key
     * @param field      hash的field
     * @param value      hash field对应的value
     * @param expireTime hash的过期时间
     */
    private static Long hsetnx(String key, String field, String value, int expireTime) {
        Long result = 0L;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hsetnx(key, field, value);
                if (expireTime > 0) {
                    jedis.expire(key, expireTime);
                }
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 值不存在则设置
     *
     * @param cacheType  缓存类型
     * @param key        hash的键
     * @param field      hash的域
     * @param value      需要设置的field的值
     * @param expireTime 过期时间
     */
    public static Long hsetnx(String cacheType, String key, String field, String value, int expireTime) {
        return hsetnx(generateFullKey(cacheType, key), field, value, expireTime);
    }

    /**
     * 获取hash key对应的所有field
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     */
    public static Set<String> hkeys(String cacheType, String key) {
        return hkeys(generateFullKey(cacheType, key));
    }

    private static Set<String> hkeys(String key) {
        Set<String> result = null;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hkeys(key);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 获取hash key对应的所有键值对
     *
     * @param prefix    自定义业务前缀
     * @param cacheType 缓存类型
     * @param key       hash的键
     */
    public static Map<String, String> hgetAll(String prefix, String cacheType, String key) {
        return hgetAll(generateFullKey(prefix, cacheType, key));
    }

    /**
     * 获取hash key对应的所有键值对
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     */
    public static Map<String, String> hgetAll(String cacheType, String key) {
        return hgetAll(generateFullKey(cacheType, key));
    }

    private static Map<String, String> hgetAll(String key) {
        Map<String, String> result = null;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hgetAll(key);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 删除hash值
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     * @param field     hash的域
     */
    public static Long hdel(String cacheType, String key, String field) {
        return hdel(generateFullKey(cacheType, key), field);
    }

    /**
     * 删除hash值
     *
     * @param key   hash的键
     * @param field hash的域
     */
    private static Long hdel(String key, String field) {
        Long result = 0L;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hdel(key, field);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * @function 给指定的key加锁，并设置锁的过期时间
     * @author ZJY 2018/07/02 20:51
     */
    public static Boolean lock(String cacheType, String key, int expireTime) {
        return lock(generateFullKey(cacheType, key), expireTime);
    }

    /**
     * @function 给指定的key加锁，使用默认的锁的过期时间
     * @author ZJY 2018/07/02 20:51
     */
    public static Boolean lock(String cacheType, String key) {
        return lock(generateFullKey(cacheType, key), LOCK_DEFAULT_EXPIRE_TIME);
    }

    /**
     * @function 给指定的key解锁
     * @author ZJY 2018/07/02 20:52
     */
    public static Boolean unlock(String cacheType, String key) {
        return unlock(generateFullKey(cacheType, key));
    }

    private static Boolean lock(String key, int expireTime) {
        Boolean result = false;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            // 防止线程饥饿
            for (int times = expireTime * 100; times > 0; times--) {
                Long status = jedis.setnx(key, "lock");
                if (1 == status) {
                    jedis.expire(key, expireTime);
                    result = true;
                    break;
                }
                Thread.sleep(10L);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    private static Boolean unlock(String key) {
        Boolean result = false;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.del(key);
            result = true;
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 判断hash值是否存在
     *
     * @param cacheType 缓存类型
     * @param key       hash的键
     * @param field     hash的域
     */
    public static Boolean hexists(String cacheType, String key, String field) {
        return hexists(generateFullKey(cacheType, key), field);
    }

    /**
     * 判断hash值是否存在
     *
     * @param key   hash的键
     * @param field hash的域
     */
    private static Boolean hexists(String key, String field) {
        Jedis jedis = null;
        Boolean result = false;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.hexists(key, field);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    public static Long lpush(String cacheType, String key, String... items) {
        return lpush(generateFullKey(cacheType, key), items);
    }

    public static Long lpush(String cacheType, String key, Object... items) {
        String[] strings = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            if (items[i] instanceof String) {
                strings[i] = (String) items[i];
            } else {
                strings[i] = JSON.toJSONString(items[i]);
            }
        }
        return lpush(generateFullKey(cacheType, key), strings);
    }

    public static String lpop(String cacheType, String key) {
        return lpop(generateFullKey(cacheType, key));
    }

    public static Long rpush(String cacheType, String key, String... items) {
        return rpush(generateFullKey(cacheType, key), items);
    }

    public static Long rpush(String cacheType, String key, Object... items) {
        String[] strings = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            if (items[i] instanceof String) {
                strings[i] = (String) items[i];
            } else {
                strings[i] = JSON.toJSONString(items[i]);
            }
        }
        return lpush(generateFullKey(cacheType, key), strings);
    }

    public static String rpop(String cacheType, String key) {
        return rpop(generateFullKey(cacheType, key));
    }

    public static List<String> lrange(String cacheType, String key, long start, long end) {
        return lrange(generateFullKey(cacheType, key), start, end);
    }

    public static List<String> lrangeAll(String cacheType, String key) {
        return lrange(generateFullKey(cacheType, key), 0, -1);
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表头
     * @param key 列表的key值
     * @param items 需要插入列表的值
     */
    private static Long lpush(String key, String... items) {
        Jedis jedis = null;
        Long result = 0L;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.lpush(key, items);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 移除并返回列表 key 的头元素
     * @param key 列表的key值
     */
    private static String lpop(String key) {
        Jedis jedis = null;
        String result = "";
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.lpop(key);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表尾
     * @param key 列表的key值
     * @param items 需要插入列表的值
     */
    private static Long rpush(String key, String... items) {
        Jedis jedis = null;
        Long result = 0L;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.rpush(key, items);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 移除并返回列表 key 的尾元素
     * @param key 列表的key值
     */
    private static String rpop(String key) {
        Jedis jedis = null;
        String result = "";
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.rpop(key);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 返回列表 key 中指定区间内的元素
     * @param key 列表的key值
     * @param start 列表的开始位置（0表示第一个元素）
     * @param end 列表的结束位置（-1表示最后一个元素）
     */
    private static List<String> lrange(String key, long start, long end) {
        Jedis jedis = null;
        List<String> result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.lrange(key, start, end);
                jedis.ltrim(key, start, end);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }


    /**
     * 为有序集 key 的成员 member 的 score 值加上增量 increment
     *
     * @param cacheType
     * @param key
     * @param score     增量increment
     * @param member    成员
     * @return
     */
    public static Double zincrby(String prefix, String cacheType, String key, double score, String member) {
        return zincrby(generateFullKey(prefix, cacheType, key), score, member);
    }

    private static Double zincrby(String key, double score, String member) {
        Jedis jedis = null;
        Double result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.zincrby(key, score, member);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，成员 member 的 score 值
     *
     * @param prefix
     * @param cacheType
     * @param key
     * @param member    成员
     * @return
     */
    public static Double zscore(String prefix, String cacheType, String key, String member) {
        return zscore(generateFullKey(prefix, cacheType, key), member);
    }

    private static Double zscore(String key, String member) {
        Jedis jedis = null;
        Double result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.zscore(key, member);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员
     *
     * @param prefix
     * @param cacheType
     * @param key
     * @param start     开始的位置
     * @param end       结束的位置
     * @return 开始-结束之间的数据
     */
    public static Set<Tuple> zrangeWithScores(String prefix, String cacheType, String key, int start, int end) {
        return zrangeWithScores(generateFullKey(prefix, cacheType, key), start, end);
    }

    private static Set<Tuple> zrangeWithScores(String key, int start, int end) {
        Jedis jedis = null;
        Set<Tuple> result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.zrangeWithScores(key, start, end);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 将一个或多个 member 元素及其 score 值加入到有序集 key 当中
     *
     * @param prefix
     * @param cacheType
     * @param key
     * @param score     分数
     * @param member    成员
     * @return
     */
    public static Long zadd(String prefix, String cacheType, String key, double score, String member) {
        return zadd(generateFullKey(prefix, cacheType, key), score, member);
    }

    private static Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.zadd(key, score, member);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * String类型 value自增1
     *
     * @param prefix
     * @param cacheType
     * @param key
     * @return
     */
    public static Long incr(String prefix, String cacheType, String key) {
        return incr(generateFullKey(prefix, cacheType, key));
    }

    private static Long incr(String key) {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = jedis.incr(key);
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 同一个redis连接的一系列操作,常用在事务等操作
     *
     * @return
     */
    public static <R> R execOnSameRedisConn(Function<RedisTransaction, R> function) {
        Jedis jedis = null;
        R result = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                result = function.apply(new RedisTransaction(jedis));
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return result;
    }

    /**
     * 获取redis 时间戳
     *
     * @return
     */
    public static Long getCurrentTimeMillisFromRedis() {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (null != jedis) {
                //time 第一位是unix时间戳 第二位是当前这一秒钟已经逝去的微秒数
                List<String> time = jedis.time();
                if (UtilCompare.isNotEmpty(time) && time.size() >= 2) {
                    //表示当前秒
                    Long second = Long.parseLong(time.get(0));
                    //表示当前这一秒钟已经逝去的微秒数
                    Long microSecond = Long.parseLong(time.get(1));
                    //计算得出当前毫秒级的时间戳
                    Long currentTimeMillis = (second * 1000) + (microSecond / 1000);
                    return currentTimeMillis;
                }
            }
        } catch (Exception e) {
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return null;
    }

    /**
     * 根据prefix, cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     *
     * @param prefix:    前缀，表示项目模块，如dw:dfs:web
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return full key，完整的存储key
     */
    public static String generateFullKey(String prefix, String cacheType, String key) {
        return MessageFormat.format("{0}:{1}:{2}", prefix, cacheType, key);
    }

    /**
     * 根据配置文件中的prefix, 及cacheType和key的值生成 完整的key如 dw:dfs:web:fileId_fileUid:2
     *
     * @param cacheType: 缓存类型，如 userId_userName, 表达了存储的key是什么类型，value是什么类型
     * @param key:       key值，如2
     * @return full key，完整的存储key
     */
    public static String generateFullKey(String cacheType, String key) {
        return MessageFormat.format("{0}:{1}:{2}", DEFAULT_KEY_PREFIX, cacheType, key);
    }

    /**
     * 获取Jedis实例
     *
     * @return
     */
    private static Jedis getJedis() {
        try {
            if (jedisPool == null) {
                init();
            }
            jedis = jedisPool.getResource();
        } catch (Exception e) {
        }
        return jedis;
    }

    public void setDefaultExpireTime(int defaultExpireTime) {
        DEFAULT_EXPIRE_TIME = defaultExpireTime;
    }

    public void setADDR(String ADDR) {
        RedisCache.ADDR = ADDR;
    }

    public void setPORT(int PORT) {
        RedisCache.PORT = PORT;
    }

    public void setAUTH(String AUTH) {
        if ("nullValue".equals(AUTH)) {
            RedisCache.AUTH = null;
        } else {
            RedisCache.AUTH = AUTH;
        }
    }

    public void setMaxActive(int maxActive) {
        MAX_ACTIVE = maxActive;
    }

    public void setMaxIdle(int maxIdle) {
        MAX_IDLE = maxIdle;
    }

    public void setMaxWait(int maxWait) {
        MAX_WAIT = maxWait;
    }

    public void setTIMEOUT(int TIMEOUT) {
        RedisCache.TIMEOUT = TIMEOUT;
    }

    public static class RedisTransaction {
        private Transaction transaction;
        private Jedis tranJedis;

        private RedisTransaction(Jedis jedis) {
            this.tranJedis = jedis;
        }

        /**
         * 监控.如果其他客户端进行了修改.则本次事务取消
         *
         * @param prefix
         * @param cacheType
         * @param key
         * @return
         */
        public String watch(String prefix, String cacheType, String key) {
            return watch(generateFullKey(prefix, cacheType, key));
        }

        private String watch(String key) {
            String result = null;
            try {
                if (null != tranJedis) {
                    result = tranJedis.watch(key);
                }
            } catch (Exception e) {
            }
            return result;
        }

        /**
         * 设置值
         *
         * @param prefix
         * @param cacheType
         * @param key
         * @param value
         * @return
         */
        public Response<String> set(String prefix, String cacheType, String key, String value) {
            return set(generateFullKey(prefix, cacheType, key), value, DEFAULT_EXPIRE_TIME);
        }

        /**
         * 设置值
         *
         * @param prefix
         * @param cacheType
         * @param key
         * @param value
         * @param expireSeconds 过期时间
         * @return
         */
        public Response<String> set(String prefix, String cacheType, String key, String value, int expireSeconds) {
            return set(generateFullKey(prefix, cacheType, key), value, expireSeconds);
        }

        private Response<String> set(String key, String value, int expireSeconds) {
            Response<String> result = null;
            try {
                result = transaction.set(key, value);
                if (expireSeconds > 0) {
                    transaction.expire(key, expireSeconds);
                }
            } catch (Exception e) {
            }
            return result;
        }

        /**
         * String类型 value自增1
         *
         * @param prefix
         * @param cacheType
         * @param key
         * @return
         */
        public Response<Long> incr(String prefix, String cacheType, String key) {
            return incr(generateFullKey(prefix, cacheType, key));
        }

        private Response<Long> incr(String key) {
            Response<Long> result = null;
            try {
                result = transaction.incr(key);
            } catch (Exception e) {
            }
            return result;
        }

        /**
         * 开启事务
         *
         * @return
         */
        public void multi() {
            try {
                this.transaction = tranJedis.multi();
            } catch (Exception e) {
            }
        }

        /**
         * 执行事务
         *
         * @return
         */
        public List<Object> exec() {
            List<Object> result = null;
            try {
                result = transaction.exec();
            } catch (Exception e) {
            }
            return result;
        }
    }

}
