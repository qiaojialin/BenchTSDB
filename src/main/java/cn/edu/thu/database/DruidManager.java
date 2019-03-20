package cn.edu.thu.database;

import cn.edu.thu.common.Record;
import java.util.List;

public class DruidManager implements IDataBaseManager {

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
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    return 0;
  }
}
