package cn.edu.thu.database.kairosdb;

import cn.edu.thu.common.ThuHttpRequest;
import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestKairosDB {

  public static void main(String... args) {

    String url = "http://127.0.0.1:1408/api/v1/datapoints";

    List<KairosDBPoint> pointList = new ArrayList<>();

      pointList.add(genOne("tag1", "tagv1", "s1", 1));
    pointList.add(genOne("tag1", "tagv1", "s1", 2));
    pointList.add(genOne("tag1", "tagv2", "s1", 1));
    pointList.add(genOne("tag1", "tagv2", "s2", 1));
    pointList.add(genOne("tag2", "tagv2", "tag3", "tagv3", "s1", 1));

    String body = JSON.toJSONString(pointList);


    String response = null;
    try {
      response = ThuHttpRequest.sendPost(url, body);
      System.out.println(response);
    } catch (IOException e) {
      e.printStackTrace();
    }


  }
  public static KairosDBPoint genOne(String tagName, String tagV, String measurement, int i){
    KairosDBPoint point = new KairosDBPoint();
    Map<String, String> tags = new HashMap<>();
    tags.put(tagName, tagV);
    point.setName(measurement);
    point.setTimestamp(System.nanoTime() + i*1000);
    point.setValue(0.1d);
    point.setTags(tags);
    return  point;
  }
  public static KairosDBPoint genOne(String tagName, String tagV, String tagName2, String tagV2, String measurement, int i){
    KairosDBPoint point = new KairosDBPoint();
    Map<String, String> tags = new HashMap<>();
    tags.put(tagName, tagV);
    tags.put(tagName2, tagV2);
    point.setName(measurement);
    point.setTimestamp(System.nanoTime() + i*1000);
    point.setValue(0.1d);
    point.setTags(tags);
    return  point;
  }
}
