import com.alibaba.fastjson.JSON;

import java.util.List;

/**
 * @author: yzh
 * @date: 2018/10/8 17:26
 * @description: 订阅方
 */
public class Consumer {

    protected static final String TOPIC_SUBSCRIBERS_CACHETYPE = "key_subscribers";
    /**
     * 默认不过期
     */
    protected static final int DEFALUT_EXPIRETIME = 0;
    /**
     * 主题总数
     */
    private RedisQueue countRedisQueue;
    /**
     * 主题消息
     */
    private RedisQueue messageRedisQueue;
    /**
     * 当前主题订阅者
     */
    private RedisQueue subscribersRedisQueue;

    public Consumer(String topic, String subscriber) {
        this.countRedisQueue = new RedisQueue(topic, Producer.TOPIC_COUNT_CACHETYPE);
        this.messageRedisQueue = new RedisQueue(topic, Producer.TOPIC_MESSAGE_CACHETYPE);
        this.subscribersRedisQueue = new RedisQueue(topic, TOPIC_SUBSCRIBERS_CACHETYPE, subscriber);
    }

    public void consume(Callback callback) {
        RedisMessageModel message = null;
        do {
            message = readUntilEnd();
            if (message != null) {
                try {
                    callback.handle(message);
                } catch (Exception e) {
                }
            }
        } while(message != null);
    }

    public RedisMessageModel consume() {
        return readUntilEnd();
    }

    /**
     * 读取队列中的消息.保证在集群环境下能正确并发读取消息
     * 1.保证watch到客户端索引自增+1之间的操作是原子操作
     *
     * @return
     */
    private RedisMessageModel readUntilEnd() {
        return RedisQueue.execOnSameRedisConn(redisTransaction -> {
            while (true) {
                redisTransaction.watch(subscribersRedisQueue);
                if (unreadMessages() > 0) {
                    String message = read();
                    if (UtilCompare.isEmpty(goNext(redisTransaction))) {
                        continue;// 自增失败重新读取
                    }
                    if (message == null) {
                        continue;// 读取失败重新读取
                    }
                    return JSON.parseObject(message, RedisMessageModel.class);
                }
                return null;
            }
        });
    }

    /**
     * 客户端消息索引+1
     * @param redisTransaction
     * @return
     */
    private List<Object> goNext(RedisQueue.RedisTransaction redisTransaction) {
        redisTransaction.multi();
        redisTransaction.incr(subscribersRedisQueue);
        return redisTransaction.exec();
    }

    /**
     * 获取当前订阅方读取的最后一条消息的位置
     * @return
     */
    private int getLastReadMessage() {
        String lastMessageRead = subscribersRedisQueue.get();
        if (UtilCompare.isEmpty(lastMessageRead)) {
            int lowest = getTopicSize() - 1;
            subscribersRedisQueue.set(String.valueOf(lowest), DEFALUT_EXPIRETIME);
            return lowest;
        }
        return Integer.valueOf(lastMessageRead);
    }

    /**
     * 消息总数量
     * @return
     */
    private int getTopicSize() {
        String stopicSize = countRedisQueue.get();
        int topicSize = 0;
        if (stopicSize != null) {
            topicSize = Integer.valueOf(stopicSize);
        }
        return topicSize;
    }

    /**
     * 读取最后一条消息
     * @return
     */
    public String read() {
        return messageRedisQueue.cat(getLastReadMessage() + 1).get();
    }

    /**
     * 可读消息的个数
     * @return
     */
    public int unreadMessages() {
        return getTopicSize() - getLastReadMessage();
    }
}
