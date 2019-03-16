import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author: yzh
 * @date: 2018/10/9 15:50
 * @description: 自定义的redisQueue
 */
public class RedisQueue {

    private static final String PREFIX = "redisMq:";
    private static final String REDIS_KEY = "all";

    private String topicPrefix;
    private String cacheType;
    private String key;
    private String sb;

    protected RedisQueue(String topic, String cacheType) {
        this.topicPrefix = PREFIX + topic;
        this.cacheType = cacheType;
    }

    protected RedisQueue(String topic, String cacheType, String key) {
        this.topicPrefix = PREFIX + topic;
        this.cacheType = cacheType;
        this.key = key;
    }

    private String key() {
        if (!UtilCompare.isEmpty(key)) {
            return key;
        }
        if (UtilCompare.isEmpty(sb)) {
            return REDIS_KEY;
        }
        String generatedKey = sb;
        sb = null;
        return generatedKey;
    }

    protected RedisQueue cat(String key) {
        this.sb = key;
        return this;
    }

    protected RedisQueue cat(Integer key) {
        this.sb = key + "";
        return this;
    }

    protected String get() {
        return RedisCache.get(topicPrefix, cacheType, key());
    }

    protected boolean del() {
        return RedisCache.del(topicPrefix, cacheType, key());
    }

    protected Set<Tuple> zrangeWithScores(int start, int end) {
        return RedisCache.zrangeWithScores(topicPrefix, cacheType, key(), start, end);
    }

    protected Double zincrby(double score, String member) {
        return RedisCache.zincrby(topicPrefix, cacheType, key(), score, member);
    }

    protected Double zscore(String member) {
        return RedisCache.zscore(topicPrefix, cacheType, key(), member);
    }

    protected Long zadd(double score, String member) {
        return RedisCache.zadd(topicPrefix, cacheType, key(), score, member);
    }

    protected String set(String value, int expireSeconds) {
        return RedisCache.set(topicPrefix, cacheType, key(), value, expireSeconds);
    }

    protected static <R> R execOnSameRedisConn(Function<RedisTransaction, R> functon) {
        return RedisCache.execOnSameRedisConn(transaction -> {
            return functon.apply(RedisQueue.RedisTransaction.initTransaction(transaction));
        });
    }

    protected static class RedisTransaction {
        private RedisCache.RedisTransaction transaction;
        private RedisTransaction(RedisCache.RedisTransaction transaction){
            this.transaction = transaction;
        }
        protected static RedisTransaction initTransaction(RedisCache.RedisTransaction transaction) {
            return new RedisTransaction(transaction);
        }

        protected Response<String> set(RedisQueue redisQueue, String value) {
            return set(redisQueue, value, 0);
        }

        protected String watch(RedisQueue redisQueue) {
            return transaction.watch(redisQueue.topicPrefix, redisQueue.cacheType, redisQueue.key());
        }

        protected Response<String> set(RedisQueue redisQueue, String value, int seconds) {
            return transaction.set(redisQueue.topicPrefix, redisQueue.cacheType, redisQueue.key(), value, seconds);
        }

        protected Response<Long> incr(RedisQueue redisQueue) {
            return transaction.incr(redisQueue.topicPrefix, redisQueue.cacheType, redisQueue.key());
        }

        protected void multi() {
            transaction.multi();
        }

        protected List<Object> exec() {
            return transaction.exec();
        }
    }
}
