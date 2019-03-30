package cn.edu.thu.database.opentsdb;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.ThuHttpRequest;
import cn.edu.thu.database.IDataBaseManager;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class OpenTSDBManager implements IDataBaseManager {

    private static final Logger logger = LoggerFactory.getLogger(OpenTSDBManager.class);
    private Config config;
    private String dbUrl;
    private String writeUrl;
    private String queryUrl;

    public OpenTSDBManager(Config config) {
        this.config = config;
        this.dbUrl = config.OPENTSDB_URL;
        this.writeUrl = this.dbUrl + "/api/put?summary";
        this.queryUrl = this.dbUrl + "/api/query";
    }

    @Override
    public void initServer() {

    }

    @Override
    public void initClient() {

    }

    @Override
    public long insertBatch(List<Record> records) {

        LinkedList<OpenTSDBPoint> openTSDBPoints = new LinkedList<>();

        for (Record record : records) {
            openTSDBPoints.addAll(convertToRecord(record));
        }

        String body = JSON.toJSONString(openTSDBPoints);

        long start = System.nanoTime();
        try {
            String response = ThuHttpRequest.sendPost(writeUrl, body);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return System.nanoTime() - start;

    }


    private LinkedList<OpenTSDBPoint> convertToRecord(Record record) {
        LinkedList<OpenTSDBPoint> models = new LinkedList<>();

        for(int i = 0; i < config.FIELDS.length; i++) {
            OpenTSDBPoint model = new OpenTSDBPoint();
            model.setMetric(config.FIELDS[i]);
            model.setTimestamp(record.timestamp);
            model.setValue(record.fields.get(i));

            Map<String, String> tags = new HashMap<>();
            tags.put(Config.TAG_NAME, record.tag);
            model.setTags(tags);
            models.addLast(model);
        }
        return models;
    }


    @Override
    public long count(String tagValue, String field, long startTime, long endTime) {
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("msResolution", true);

        Map<String, Object> subQuery = new HashMap<>();

        // query tag
        Map<String, String> subsubQuery = new HashMap<>();
        subsubQuery.put(Config.TAG_NAME, tagValue);
        subQuery.put("tags", subsubQuery);

        if(startTime == -1 || endTime == -1) {
            logger.error("do not support");
            return -1;
        } else {
            long diff = endTime - startTime;
            queryMap.put("start", startTime);
            subQuery.put("downsample", (diff) + "ms-count");

        }

        subQuery.put("metric", field);
        subQuery.put("aggregator", "none");

        List<Map<String, Object>> queries = new ArrayList<>();
        queries.add(subQuery);
        queryMap.put("queries", queries);

        String sql = JSON.toJSONString(queryMap);

        logger.info("sql: {}", sql);
        long start = System.nanoTime();

        try {
            String response = ThuHttpRequest.sendPost(queryUrl, sql);
            logger.info(response);
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
