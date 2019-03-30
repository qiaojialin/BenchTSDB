package cn.edu.thu.database;

import cn.edu.thu.common.Record;

import java.util.List;

public interface IDataBaseManager {

    /**
     * @return time cost in ns
     */
    long insertBatch(List<Record> records);


    /**
     * init server once in main thread
     * may drop table, create necessary schema
     * no need to close the manager
     */
    void initServer();


    /**
     * init each client in child thread
     */
    void initClient();


    /**
     *
     * @param tagValue queried deviceId
     * @param field queried field
     * @return time cost in ns
     */
    long count(String tagValue, String field, long startTime, long endTime);

    /**
     * @return time cost in ns
     */
    long flush();

    /**
     * @return time cost in ns
     */
    long close();

}
