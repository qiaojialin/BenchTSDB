package cn.edu.thu.datasource.parser;

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

public class TDriveParser implements IParser{

  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  private static Logger logger = LoggerFactory.getLogger(TDriveParser.class);


  @Override
  public List<Record> parse(String fileName) {

    List<Record> records = new ArrayList<>();

    try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

      // skip 6 lines, which is useless
      for(int i = 0; i < 6; i++) {
        reader.readLine();
      }

      String line;

      while ((line = reader.readLine()) != null) {
        try {
          Record record = convertToRecord(line);
          records.add(record);
        } catch (ParseException ignored) {
        }
      }

    } catch (Exception e) {
      logger.warn("parse {} failed, because {}", fileName, e.getMessage());
      e.printStackTrace();
    }

    return records;
  }

  @Override
  public void close() {

  }

  private Record convertToRecord(String line) throws ParseException {

    List<Object> fields = new ArrayList<>();

    String[] items = line.split(",");

    fields.add(Double.parseDouble(items[2]));
    fields.add(Double.parseDouble(items[3]));

    Date date = dateFormat.parse(items[1]);
    long time = date.getTime();

    return new Record(time, items[0], fields);

  }

}
