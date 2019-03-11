package cn.edu.thu.manager;

import cn.edu.thu.common.Record;

import java.util.List;
import java.util.Map;

public class NullManager implements IDataBase{

    public NullManager() {

    }

    @Override
    public long insertBatch(List<Record> records) {
        return 0;
    }

    @Override
    public void createSchema() {

    }

    @Override
    public long count(String tagValue, String field, long startTime, long endTime) {
        return 0;
    }

    @Override
    public long close() {
        return 0;
    }
}
