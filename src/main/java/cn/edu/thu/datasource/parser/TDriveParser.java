package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TDriveParser implements IParser{

  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

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

    } catch (IOException e) {
      e.printStackTrace();
    }

    return records;
  }

  private Record convertToRecord(String line) throws ParseException {

    List<Object> fields = new ArrayList<>();

    String[] items = line.split(",");

    fields.add(Float.parseFloat(items[2]));
    fields.add(Float.parseFloat(items[3]));

    Date date = dateFormat.parse(items[1]);
    long time = date.getTime();

    return new Record(time, items[0], fields);

  }

}
