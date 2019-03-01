package cn.edu.thu.manager;

import cn.edu.thu.Record;
import cn.edu.thu.conf.Config;

import java.util.List;

public class OpenTSDBManager implements IDBManager {

    private Config config;

    public OpenTSDBManager(Config config) {
        this.config = config;
    }

    @Override
    public void process(List<Record> records) {

    }

    @Override
    public void createSchema() {

    }
}
