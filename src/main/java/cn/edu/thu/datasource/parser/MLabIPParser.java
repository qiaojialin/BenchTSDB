package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
        if (fileName.contains("dash")) {
          records.addAll(convertToRecords(line));
        } else if (fileName.contains("raw")) {
          records.add(convertToRecord2(line));
        } else {
          // speedtest and bittorrent file
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
   * parse dash file
   */
  private List<Record> convertToRecords(String line) {
    List<Record> records = new ArrayList<>();
    try {
      JSONObject jsonObject = JSON.parseObject(line);

      JSONArray clients = jsonObject.getJSONArray("client");
      for (int i = 0; i < clients.size(); i++) {
        JSONObject client = clients.getJSONObject(i);
        String ip = client.getString("real_address");
        long time = client.getLongValue("timestamp");
        List<Object> fields = new ArrayList<>();
        fields.add(client.getFloatValue("connect_time"));
        records.add(new Record(time, ip, fields));
      }
    } catch (Exception ignore) {
      logger.warn("can not parse: {}", line);
      logger.warn("exception: {}", ignore.getMessage());
    }
    return records;
  }

  /**
   * parse speedtest and bittorrent file
   */
  private Record convertToRecord1(String line) {

    JSONObject jsonObject = JSON.parseObject(line);

    String ip = jsonObject.getString("real_address");
    long time = jsonObject.getLongValue("timestamp");

    List<Object> fields = new ArrayList<>();
    for (String field : config.FIELDS) {
      fields.add(jsonObject.getFloatValue(field));
    }
    return new Record(time, ip, fields);
  }

  /**
   * parse raw file
   */
  private Record convertToRecord2(String line) {
    JSONObject jsonObject = JSON.parseObject(line);

    String ip = jsonObject.getJSONObject("client").getString("myname");
    long time = jsonObject.getJSONObject("server").getLongValue("timestamp");

    List<Object> fields = new ArrayList<>();
    float value = jsonObject.getJSONObject("client").getFloatValue("connect_time");
    fields.add(value);

    return new Record(time, ip, fields);
  }
}
