package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import com.google.gson.JsonArray;
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

    // cannot parse this type of file
    if(fileName.contains("_raw")) {
      return records;
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

      String line;

      while ((line = reader.readLine()) != null) {
        if(fileName.contains("_dash")) {
          records.addAll(convertToRecords(line));
        } else {
          records.add(convertToRecord(line));
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return records;
  }

  /**
   * parse _dash file
   * @return
   */
  private List<Record> convertToRecords(String line) {
    List<Record> records = new ArrayList<>();
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();

    JsonArray clients = jsonObject.get("client").getAsJsonArray();
    for(int i = 0; i < clients.size(); i++) {
      JsonObject client = clients.get(i).getAsJsonObject();
      String ip = client.get("real_address").getAsString();
      long time = client.get("timestamp").getAsLong();
      List<Object> fields = new ArrayList<>();
      fields.add(client.get("connect_time").getAsFloat());
      records.add(new Record(time, ip, fields));
    }
    return records;
  }

  /**
   * parse _speedtest and _bittorrent file
   * @param line
   * @return
   */
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
