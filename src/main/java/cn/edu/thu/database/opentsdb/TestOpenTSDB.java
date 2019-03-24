package cn.edu.thu.database.opentsdb;

import cn.edu.thu.common.ThuHttpRequest;
import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestOpenTSDB {

  private static String writeUrl = "http://127.0.0.1:4242/api/put?summary";
  private static String queryUrl = "http://127.0.0.1:4242/api/query";

  public static void main(String... args) {

//    insert();

//    query("metric1", 1552821047000L, 1552821050000L);
    query("metric1", 0L, 9000000L);

  }

  private static void query(String metric, long startTime, long endTime) {
    Map<String, Object> queryMap = new HashMap<>();
    queryMap.put("msResolution", true);

    Map<String, Object> subQuery = new HashMap<>();

    // query tag
    Map<String, String> subsubQuery = new HashMap<>();
    subsubQuery.put("deviceId", "d1");
    subQuery.put("tags", subsubQuery);

    queryMap.put("start", startTime);
    queryMap.put("end", endTime);
    subQuery.put("downsample", (endTime-startTime) + "ms-count");

    subQuery.put("metric", metric);
    subQuery.put("aggregator", "none");

    List<Map<String, Object>> queries = new ArrayList<>();
    queries.add(subQuery);
    queryMap.put("queries", queries);

    String sql = JSON.toJSONString(queryMap);

    System.out.println(sql);

    try {
      String response = ThuHttpRequest.sendPost(queryUrl, sql);
      System.out.println(response);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void insert() {
    LinkedList<OpenTSDBPoint> models = new LinkedList<>();

    for (int i = 100; i < 1000; i = i + 100) {
      OpenTSDBPoint model = new OpenTSDBPoint();
      model.setMetric("metric1");
      model.setTimestamp(i);
      model.setValue(i);

      Map<String, String> tags = new HashMap<>();
      tags.put("deviceId", "d1");
      model.setTags(tags);
      models.addLast(model);

    }

//    for (int i = 100; i < 1000; i = i + 10) {
//      OpenTSDBPoint model = new OpenTSDBPoint();
//      model.setMetric("metric2");
//      model.setTimestamp(i);
//      model.setValue(i);
//
//      Map<String, String> tags = new HashMap<>();
//      tags.put("deviceId", "d1");
//      model.setTags(tags);
//      models.addLast(model);
//
//    }

    String body = JSON.toJSONString(models);

    try {
      String response = ThuHttpRequest.sendPost(writeUrl, body);
      System.out.println(response);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
