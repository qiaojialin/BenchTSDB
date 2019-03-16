package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MLabIPParser implements IParser {

  private Config config;

  public MLabIPParser(Config config) {
    this.config = config;
  }

  @Override
  public List<Record> parse(String fileName) {
    List<Record> records = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

      String line;

      while ((line = reader.readLine()) != null) {
        records.add(convertToRecord(line));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return records;
  }


  private Record convertToRecord(String line) {
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();

    String ip = jsonObject.get("real_address").getAsString();
    long time = jsonObject.get("timestamp").getAsLong();

    List<Object> fields = new ArrayList<>();
    for (String field : config.FIELDS) {
      fields.add(jsonObject.get(field).getAsFloat());
    }

    return new Record(time, ip, fields);
  }
}
