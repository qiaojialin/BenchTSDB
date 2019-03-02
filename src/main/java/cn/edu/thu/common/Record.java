package cn.edu.thu.common;

import java.util.List;

public class Record {

    public long timestamp;

    public String deviceId;

    public List<Object> fields;

    public Record(long timestamp, String deviceId, List<Object> fields) {
        this.timestamp = timestamp;
        this.deviceId = deviceId;
        this.fields = fields;
    }



}
