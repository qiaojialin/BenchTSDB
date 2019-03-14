package cn.edu.thu.database;

import cn.edu.thu.common.Record;

import java.io.IOException;
import java.util.List;

public interface IDataBaseM {

    /**
     * @return time cost in ms
     */
    long insertBatch(List<Record> records);

    void createSchema();

    long count(String tagValue, String field, long startTime, long endTime);

    long flush();

    long close();

}
