package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReddReader extends BasicReader {

  public ReddReader(Config config, List<String> files) {
    super(config, files);
  }

  @Override
  public void init() {
    currentDeviceId = currentFile.split(config.DATA_DIR)[1].replace(".dat", "").replace("/", "_");
  }

  @Override
  public List<Record> nextBatch() {
    List<Record> records = new ArrayList<>();
    for (String line : cachedLines) {
      Record record = convertToRecord(line);
      if (record != null) {
        records.add(record);
      }
    }
    return records;
  }

  private Record convertToRecord(String line) {
    try {
      List<Object> fields = new ArrayList<>();
      String[] items = line.split(" ");
      long time = Long.parseLong(items[0]) * 1000;
      double value = Double.parseDouble(items[1]);
      fields.add(value);
      return new Record(time, currentDeviceId, fields);
    } catch (Exception ignore) {
      ignore.printStackTrace();
    }
    return null;
  }
}
