@FunctionalInterface
public interface Callback {
    /**
     * 处理消息
     * @param message
     */
    void handle(RedisMessageModel message);
}
