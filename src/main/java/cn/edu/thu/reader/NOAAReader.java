package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class NOAAReader extends BasicReader {

  private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

  public NOAAReader(Config config, List<String> files) {
    super(config, files);
  }


  private Record convertToRecord(String line) {

    try {

      List<Object> fields = new ArrayList<>();

      String tag = line.substring(0, 6).trim() + "_" + line.substring(7, 12).trim();
      //add 70 years, make sure time > 0
      String yearmoda = line.substring(14, 22).trim();
      Date date = dateFormat.parse(yearmoda);
      long time = date.getTime() + 2209046400000L;

      fields.add(Double.parseDouble(line.substring(24, 30).trim()));
      fields.add(Double.parseDouble(line.substring(35, 41).trim()));
      fields.add(Double.parseDouble(line.substring(46, 52).trim()));
      fields.add(Double.parseDouble(line.substring(57, 63).trim()));
      fields.add(Double.parseDouble(line.substring(68, 73).trim()));
      fields.add(Double.parseDouble(line.substring(78, 83).trim()));
      fields.add(Double.parseDouble(line.substring(88, 93).trim()));
      fields.add(Double.parseDouble(line.substring(95, 100).trim()));
      fields.add(Double.parseDouble(line.substring(102, 108).trim()));
      fields.add(Double.parseDouble(line.substring(110, 116).trim()));
      fields.add(Double.parseDouble(line.substring(118, 123).trim()));
      fields.add(Double.parseDouble(line.substring(125, 130).trim()));
      fields.add(Double.parseDouble(line.substring(132, 138).trim()));

      return new Record(time, tag, fields);
    } catch (Exception ingore) {
      return null;
    }
  }

  @Override
  public void init() throws Exception {
    // skip first line, which is the metadata
    reader.readLine();
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
}
