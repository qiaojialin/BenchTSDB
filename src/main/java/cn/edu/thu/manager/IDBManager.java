package cn.edu.thu.manager;

import cn.edu.thu.Record;

import java.util.List;

public interface IDBManager {

    void process(List<Record> records);

    void createSchema();

}
