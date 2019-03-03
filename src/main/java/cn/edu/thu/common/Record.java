package cn.edu.thu.common;

import java.util.List;

public class Record {

    public long timestamp;

    public String tag1;

    public String tag2;

    public List<Object> fields;

    public Record(long timestamp, String tag1, String tag2, List<Object> fields) {
        this.timestamp = timestamp;
        this.tag1 = tag1;
        this.tag2 = tag2;
        this.fields = fields;
    }



}
