package cn.edu.thu.common;

import java.util.List;
import java.util.Map;

public class Record {

    public long timestamp;

    public String tag;

    public List<Object> fields;

    public Record(long timestamp, String tag, List<Object> fields) {
        this.timestamp = timestamp;
        this.tag = tag;
        this.fields = fields;
    }

}
