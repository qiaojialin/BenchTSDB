package cn.edu.thu.manager;

import cn.edu.thu.common.Record;

import java.util.List;

public interface IDataBase {

    /**
     * @return time cost in ms
     */
    long insertBatch(List<Record> records);

    void createSchema();

    long count(String tag1, String tag2, String field, long startTime, long endTime);

    long close();

}
