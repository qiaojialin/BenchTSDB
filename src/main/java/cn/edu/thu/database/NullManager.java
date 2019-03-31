package cn.edu.thu.database;

import cn.edu.thu.common.Record;

import cn.edu.thu.database.IDataBaseManager;
import java.util.List;

public class NullManager implements IDataBaseManager {

    public NullManager() {

    }

    @Override
    public long insertBatch(List<Record> records) {
        return 0;
    }

    @Override
    public void initServer() {

    }

    @Override
    public void initClient() {

    }

    @Override
    public long count(String tagValue, String field, long startTime, long endTime) {
        return 0;
    }

    @Override
    public long flush() {
        return 0;
    }

    @Override
    public long close() {
        return 0;
    }
}
