package com.zto.fire.flink.bean;

/**
 * checkpoint热修改参数
 * {"interval":10000, "timeout":20000, "minPauseBetween": 10000}
 * @author ChengLong 2019-5-5 16:57:49
 */
public class CheckpointParams {

    /**
     * checkpoint的频率
     */
    private Long interval;

    /**
     * checkpoint的超时时间
     */
    private Long timeout;

    /**
     * 两次checkpoint的最短时间间隔
     */
    private Long minPauseBetween;

    public Long getInterval() {
        return interval;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Long getMinPauseBetween() {
        return minPauseBetween;
    }

    public void setMinPauseBetween(Long minPauseBetween) {
        this.minPauseBetween = minPauseBetween;
    }
}
