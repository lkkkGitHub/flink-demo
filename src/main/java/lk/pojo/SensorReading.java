package lk.pojo;

/**
 * @author lk
 * 2021/7/21 7:50
 */
public class SensorReading {
    private Integer id;
    private Long timestamp;
    private Integer num;

    public SensorReading() {
    }

    public SensorReading(Integer id, Long timestamp, Integer num) {
        this.id = id;
        this.timestamp = timestamp;
        this.num = num;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public SensorReading(Long timestamp, Integer num) {
        this.timestamp = timestamp;
        this.num = num;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}
