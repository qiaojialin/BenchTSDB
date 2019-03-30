//package cn.edu.thu.datasource.reader;
//
//import cn.edu.thu.common.Config;
//import cn.edu.thu.common.Record;
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import java.util.ArrayList;
//import java.util.List;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class MLabIPParser extends BasicReader {
//
//  private Config config;
//  private static Logger logger = LoggerFactory.getLogger(MLabIPParser.class);
//
//  public MLabIPParser(Config config, List<String> files) {
//    super(config, files);
//  }
//
//
//  /**
//   * parse dash file
//   */
//  private List<Record> convertToRecords(String line) {
//    List<Record> records = new ArrayList<>();
//    try {
//      JSONObject jsonObject = JSON.parseObject(line);
//      JSONArray clients = jsonObject.getJSONArray("client");
//      for (int i = 0; i < clients.size(); i++) {
//        JSONObject client = clients.getJSONObject(i);
//        String ip = client.getString("real_address");
//        long time = client.getLongValue("timestamp");
//        List<Object> fields = new ArrayList<>();
//        fields.add(client.getDoubleValue("connect_time"));
//        records.add(new Record(time, ip, fields));
//      }
//    } catch (Exception ignore) {
//      logger.warn("can not parse: {}", line);
//      logger.warn("exception: {}", ignore.getMessage());
//    }
//    return records;
//  }
//
//  /**
//   * parse speedtest and bittorrent file
//   */
//  private Record convertToRecord1(String line) {
//
//    JSONObject jsonObject = JSON.parseObject(line);
//
//    String ip = jsonObject.getString("real_address");
//    long time = jsonObject.getLongValue("timestamp");
//
//    List<Object> fields = new ArrayList<>();
//    for (String field : config.FIELDS) {
//      fields.add(jsonObject.getDoubleValue(field));
//    }
//    return new Record(time, ip, fields);
//  }
//
//  /**
//   * parse raw file
//   */
//  private Record convertToRecord2(String line) {
//    JSONObject jsonObject = JSON.parseObject(line);
//
//    String ip = jsonObject.getJSONObject("client").getString("myname");
//    long time = jsonObject.getJSONObject("server").getLongValue("timestamp");
//
//    List<Object> fields = new ArrayList<>();
//    double value = jsonObject.getJSONObject("client").getDoubleValue("connect_time");
//    fields.add(value);
//
//    return new Record(time, ip, fields);
//  }
//
//  @Override
//  void init() throws Exception {
//
//  }
//
//  @Override
//  public List<Record> nextBatch() {
//    List<Record> records = new ArrayList<>();
//    String fileName = files.get(currentFileIndex);
//    for(String line: cachedLines) {
//      if (fileName.contains("dash")) {
//        records.addAll(convertToRecords(line));
//      } else if (fileName.contains("raw")) {
//        records.add(convertToRecord2(line));
//      } else {
//        records.add(convertToRecord1(line));
//      }
//    }
//    return records;
//  }
//}
