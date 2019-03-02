package cn.edu.thu.manager;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.OpenTSDBPoint;
import cn.edu.thu.common.ThuHttpRequest;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class OpenTSDB implements IDataBase {

    private static final Logger logger = LoggerFactory.getLogger(OpenTSDB.class);
    private Config config;
    private String dbUrl;
    private String writeUrl;
    private String queryUrl;

    public OpenTSDB(Config config) {
        this.config = config;
        this.dbUrl = config.OPENTSDB_URL;
        this.writeUrl = this.dbUrl + "/api/put?summary ";
        this.queryUrl = this.dbUrl + "/api/query";
    }

    @Override
    public void insertBatch(List<Record> records) {

        LinkedList<OpenTSDBPoint> openTSDBPoints = new LinkedList<>();

        for (Record record : records) {
            openTSDBPoints.addAll(convertToRecord(record));
        }

        String body = JSON.toJSONString(openTSDBPoints);
        try {

            String response = ThuHttpRequest.sendPost(writeUrl, body);
            logger.info(response);

        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }


    private LinkedList<OpenTSDBPoint> convertToRecord(Record record) {
        LinkedList<OpenTSDBPoint> models = new LinkedList<>();

        for(int i = 0; i < config.FIELDS.length; i++) {
            OpenTSDBPoint model = new OpenTSDBPoint();
            model.setMetric(config.FIELDS[i]);
            model.setTimestamp(record.timestamp);
            model.setValue(record.fields.get(i));
            Map<String, String> tags = new HashMap<>();
            tags.put(config.DEVICE_ID, record.deviceId);
            model.setTags(tags);
            models.addLast(model);
        }
        return models;
    }


    @Override
    public void createSchema() {

    }

    @Override
    public void count(String deviceId, String field, long startTime, long endTime) {
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("msResolution", true);

        Map<String, Object> subQuery = new HashMap<>();

        Map<String, Object> subsubQuery = new HashMap<>();
        subsubQuery.put(config.DEVICE_ID, deviceId);
        subQuery.put("tags", subsubQuery);

        if(startTime == -1 || endTime == -1) {
            queryMap.put("start", 0);
            queryMap.remove("end");
            subQuery.put("downsample", (100000L) + "ms-count");
        } else {
            queryMap.put("start", startTime - 1);
            queryMap.put("end", endTime + 1);
            subQuery.put("downsample", (endTime - startTime + 1) + "ms-count");

        }

        subQuery.put("metric", field);
        subQuery.put("aggregator", "count");

        List<Map<String, Object>> queries = new ArrayList<>();
        queries.add(subQuery);
        queryMap.put("queries", queries);

        String sql = JSON.toJSONString(queryMap);
        try {
            String response = ThuHttpRequest.sendPost(queryUrl, sql);
            logger.debug(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
