package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TDriveParser extends BasicParser {

  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  private static Logger logger = LoggerFactory.getLogger(TDriveParser.class);

  public TDriveParser(Config config, List<String> files) {
    super(config, files);
  }

  @Override
  public void init() {

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

      String[] items = line.split(",");

      fields.add(Double.parseDouble(items[2]));
      fields.add(Double.parseDouble(items[3]));

      Date date = dateFormat.parse(items[1]);
      long time = date.getTime();

      return new Record(time, items[0], fields);
    } catch (Exception ignore) {
    }
    return null;
  }

}
