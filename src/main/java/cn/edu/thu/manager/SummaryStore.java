package cn.edu.thu.manager;

import cn.edu.thu.common.Record;

import java.util.List;

public class SummaryStore implements IDataBase {
    @Override
    public long insertBatch(List<Record> records) {

        long start = System.currentTimeMillis();

        return System.currentTimeMillis() - start;

    }

    @Override
    public void createSchema() {

    }

    @Override
    public void count(String tag1, String tag2, String field, long startTime, long endTime) {

    }

}
