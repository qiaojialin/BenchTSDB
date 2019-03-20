package cn.edu.thu.database;

import cn.edu.thu.common.Record;

import java.util.List;

public interface IDataBaseManager {

    /**
     * @return time cost in ms
     */
    long insertBatch(List<Record> records);

    void createSchema();

    long count(String tagValue, String field, long startTime, long endTime);

    long flush();

    long close();

}
