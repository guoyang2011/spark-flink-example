package com.fastbird.streaming.flink;

import java.io.Serializable;

/**
 * Created by yangguo on 2018/8/10.
 */
public class KeyValue implements Serializable{
    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


}
