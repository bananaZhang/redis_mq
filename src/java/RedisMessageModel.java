import java.io.Serializable;
import java.util.Date;

/**
 * @author: yzh
 * @date: 2018/10/10 20:03
 * @description: 中间转换层
 */
public class RedisMessageModel implements Serializable{
    private static final long serialVersionUID = 3932005310385815528L;
    /** 主键id */
    private Integer id;
    /** 创建时间 */
    private Date createTime;
    /** 更新时间 */
    private Date updateTime;
    /** 消息内容 */
    private String content;
    /** 消息主题 */
    private String topic;
    /** 附加信息 */
    private String extraInfo;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    @Override public String toString() {
        return "RedisMessageModel{" + "id=" + id + ", createTime=" + createTime + ", updateTime=" + updateTime
            + ", content='" + content + '\'' + ", topic='" + topic + '\'' + ", extraInfo='" + extraInfo + '\'' + '}';
    }
}
