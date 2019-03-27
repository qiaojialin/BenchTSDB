package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeolifeParser implements IParser {

  private static Logger logger = LoggerFactory.getLogger(GeolifeParser.class);
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss");
  private String tag = "";
  private String fileName = "";


  @Override
  public List<Record> parse(String fileName) {

    this.fileName = fileName;

    // skip error files
    if (fileName.contains("lables.txt")) {
      return new ArrayList<>();
    }

    //replace("/Trajectory/", "_").replace(".plt", "").replace("/", "")

    tag = fileName.split("geolife/")[1].split("/Trajectory")[0];

    List<Record> records = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

      // skip 6 lines, which is useless
      for (int i = 0; i < 6; i++) {
        reader.readLine();
      }

      String line;

      while ((line = reader.readLine()) != null) {
        Record record = convertToRecord(line);
        if (record != null) {
          records.add(record);
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

  private Record convertToRecord(String line) {
    try {
      List<Object> fields = new ArrayList<>();
      String[] items = line.split(",");

      fields.add(Double.parseDouble(items[0]));
      fields.add(Double.parseDouble(items[1]));
      fields.add(Double.parseDouble(items[2]));
      fields.add(Double.parseDouble(items[3]));

      Date date = dateFormat.parse(items[5] + "-" + items[6]);
      long time = date.getTime();
      return new Record(time, tag, fields);
    } catch (Exception ignore) {
      logger.warn("can not parse: {}, error message: {}, File name: {}", line, ignore.getMessage(),
          fileName);
    }
    return null;
  }

}
