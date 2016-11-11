package com.bj58.pay.aof.model;

/**
 * @author <a href="mailto:yangjing06@58ganji.com">yangjing06</a>
 * @since 16/11/9 下午3:27
 * @Company 58.com
 * @version 1.0
 */
public class Pairs {
    private Long userId;
    private Long id;

    public Long getUserId() {
        return userId;
    }

    public Long getId() {
        return id;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override public String toString() {
        return "Pairs{" + "userId=" + userId + ", id=" + id + '}';
    }
}
