/**
 * @author: yzh
 * @date: 2018/10/9 11:07
 * @description: 回调方法
 */
@FunctionalInterface
public interface Callback {
    /**
     * 处理消息
     * @param message
     */
    void handle(RedisMessageModel message);
}
