package cn.edu.thu.manager;

import cn.edu.thu.common.Record;

import java.util.List;

public interface IDataBase {

    void insertBatch(List<Record> records);

    void createSchema();

    void count(String deviceId, String field, long startTime, long endTime);

}
