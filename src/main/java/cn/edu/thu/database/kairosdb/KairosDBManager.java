package cn.edu.thu.database.kairosdb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.ThuHttpRequest;
import cn.edu.thu.database.IDataBaseManager;
import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KairosDBManager implements IDataBaseManager {

  private static final Logger logger = LoggerFactory.getLogger(KairosDBManager.class);
  private Config config;
  private String queryUrl;
  private String writeUrl;
  private String deleteUrl;
  private String url;

  private static final String QUERY_START_TIME = "start_absolute";
  private static final String QUERY_END_TIME = "end_absolute";

  public KairosDBManager(Config config) {
    this.config = config;
    this.url = config.KAIROSDB_URL;
    queryUrl = url + "/api/v1/datapoints/query";
    writeUrl = url + "/api/v1/datapoints";
    deleteUrl = url + "/api/v1/metric/%s";
  }

  @Override
  public void initServer() {
    for (String sensor : config.FIELDS) {
      try {
        ThuHttpRequest.sendDelete(String.format(deleteUrl, sensor), "");
      } catch (IOException e) {
        logger.error("Delete metric {} failed when initializing KairosDBManager.", sensor);
        e.printStackTrace();
      }
    }
  }

  @Override
  public void initClient() {

  }

  @Override
  public long insertBatch(List<Record> records) {
    List<KairosDBPoint> points = new ArrayList<>();

    // convert to kairosdb data points
    for (Record record : records) {
      points.addAll(convertToPoints(record));
    }
    String body = JSON.toJSONString(points);

    long start = System.nanoTime();

    String response = null;
    try {
      response = ThuHttpRequest.sendPost(writeUrl, body);
      logger.info("response: {}", response);
    } catch (IOException e) {
      e.printStackTrace();
      logger.error("meet error when writing: {}", e.getMessage());
    }

    return System.nanoTime() - start;
  }

  private List<KairosDBPoint> convertToPoints(Record record) {
    List<KairosDBPoint> points = new ArrayList<>();

    Map<String, String> tags = new HashMap<>();
    tags.put(Config.TAG_NAME, record.tag);
    for (int i = 0; i < config.FIELDS.length; i++) {
      KairosDBPoint point = new KairosDBPoint();
      point.setName(config.FIELDS[i]);
      point.setTimestamp(record.timestamp);
      point.setValue(record.fields.get(i));
      point.setTags(tags);
      points.add(point);
    }
    return points;
  }


  /**
   * {
   *    "start_absolute": 1357023600000,
   *    "end_relative": {
   *        "value": "5",
   *        "unit": "days"
   *    },
   *    "time_zone": "Asia/Kabul",
   *    "metrics": [
   *        {
   *            "tags": {
   *                "host": ["foo", "foo2"],
   *                "customer": ["bar"]
   *            },
   *            "name": "abc.123",
   *            "limit": 10000,
   *            "aggregators": [
   *                {
   *                    "name": "sum",
   *                    "sampling": {
   *                        "value": 10,
   *                        "unit": "minutes"
   *                    }
   *                }
   *            ]
   *        },
   *        {
   *            "tags": {
   *                "host": ["foo", "foo2"],
   *                "customer": ["bar"]
   *            },
   *            "name": "xyz.123",
   *            "aggregators": [
   *                {
   *                    "name": "avg",
   *                    "sampling": {
   *                        "value": 10,
   *                        "unit": "minutes"
   *                    }
   *                }
   *            ]
   *        }
   *    ]
   * }
   *
   */
  @Override public long count(String tagValue, String field, long startTime, long endTime) {

    Map<String, Object> queryMap = new HashMap<>();

    if (startTime == -1 || endTime == -1) {
      queryMap.put(QUERY_START_TIME, 0);
      queryMap.put(QUERY_END_TIME, Long.MAX_VALUE);
    } else {
      queryMap.put(QUERY_START_TIME, startTime);
      queryMap.put(QUERY_END_TIME, endTime);
    }

    List<Map<String, Object>> subQueries = new ArrayList<>();

    Map<String, Object> subQuery = new HashMap<>();
    subQuery.put("name", field);

    Map<String, List<String>> tags = new HashMap<>();
    List<String> tagVs = new ArrayList<>();
    tagVs.add(tagValue);
    tags.put(Config.TAG_NAME, tagVs);
    subQuery.put("tags", tags);

    List<Map<String, Object>> aggregators = new ArrayList<>();

    Map<String, Object> aggregator = new HashMap<>();
    aggregator.put("name", "count");

    Map<String, Object> sampling = new HashMap<>();
    sampling.put("value", 1000);
    sampling.put("unit", "years");

    aggregator.put("sampling", sampling);

    aggregators.add(aggregator);
    subQuery.put("aggregators", aggregators);

    subQueries.add(subQuery);

    queryMap.put("metrics", subQueries);

    String json = JSON.toJSONString(queryMap);

    logger.info("sql：{}", json);

    long start = System.nanoTime();
    try {
      String response = ThuHttpRequest.sendPost(queryUrl, json);
      logger.info("result: {}", response);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    return 0;
  }
}
