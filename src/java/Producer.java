import com.alibaba.fastjson.JSON;

import java.util.List;

/**
 * 生产方
 */
public class Producer {
    protected static final String TOPIC_MESSAGE_CACHETYPE = "queueId_message";
    protected static final String TOPIC_COUNT_CACHETYPE = "key_count";
    /**
     * 默认过期时间1天
     */
    protected static final int DEFALUT_EXPIRETIME = 60 * 60 * 24;

    /**
     * 主题总数队列
     */
    private RedisQueue countRedisQueue;
    /**
     * 主题消息队列
     */
    private RedisQueue messageRedisQueue;

    public Producer(String topic) {
        this.countRedisQueue = new RedisQueue(topic, TOPIC_COUNT_CACHETYPE);
        this.messageRedisQueue = new RedisQueue(topic, TOPIC_MESSAGE_CACHETYPE);
    }

    /**
     * 发布消息,消息默认不过期
     * @param message 消息
     */
    public boolean publish(final RedisMessageModel message) {
        return publish(message, DEFALUT_EXPIRETIME);
    }

    /**
     * 发布消息,指定过期时间
     * @param message 消息
     * @param seconds 过期时间
     */
    public boolean publish(final RedisMessageModel message, int seconds) {
        //在redis事务下发布相关消息
        return exec(JSON.toJSONString(message), seconds) != null;
    }

    private List<Object> exec(final String message, int seconds) {
        return RedisQueue.execOnSameRedisConn(redisTransaction -> {
            //监控消息总条数.如果其他客户端进行了修改.则本次事务取消
            redisTransaction.watch(countRedisQueue);
            Integer lastMessageId = getNextMessageId();
            redisTransaction.multi();
            //记录消息总条数
            redisTransaction.set(countRedisQueue, lastMessageId.toString());
            //具体消息
            redisTransaction.set(messageRedisQueue.cat(lastMessageId), message, seconds);
            return redisTransaction.exec();
        });
    }

    /**
     * 获取下一条消息id
     * @return
     */
    protected Integer getNextMessageId() {
        final String slastMessageId = countRedisQueue.get();
        Integer lastMessageId = 0;
        if (slastMessageId != null) {
            lastMessageId = Integer.parseInt(slastMessageId);
        }
        lastMessageId++;
        return lastMessageId;
    }

}
