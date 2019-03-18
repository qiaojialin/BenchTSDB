package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLabIPParser implements IParser {

  private Config config;
  private static Logger logger = LoggerFactory.getLogger(MLabIPParser.class);

  public MLabIPParser(Config config) {
    this.config = config;
  }

  @Override
  public List<Record> parse(String fileName) {
    List<Record> records = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

      String line;

      while ((line = reader.readLine()) != null) {
        if (fileName.contains("_dash")) {
          records.addAll(convertToRecords(line));
        } else if (fileName.contains("_raw")) {
          records.add(convertToRecord2(line));
        } else {
          records.add(convertToRecord1(line));
        }
      }

    } catch (Exception e) {
      logger.warn("parse {} failed, because {}", fileName, e.getMessage());
      e.printStackTrace();
    }

    return records;
  }

  /**
   * parse _dash file
   */
  private List<Record> convertToRecords(String line) {
    List<Record> records = new ArrayList<>();
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();

    JsonArray clients = jsonObject.get("client").getAsJsonArray();
    for (int i = 0; i < clients.size(); i++) {
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
   */
  private Record convertToRecord1(String line) {

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

  /**
   * parse _raw file
   */
  private Record convertToRecord2(String line) {
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();

    String ip = jsonObject.get("client").getAsJsonObject().get("myname").getAsString();
    long time = jsonObject.get("server").getAsJsonObject().get("timestamp").getAsLong();

    List<Object> fields = new ArrayList<>();
    float value = jsonObject.get("client").getAsJsonObject().get("connect_time").getAsFloat();
    fields.add(value);

    return new Record(time, ip, fields);
  }
}
