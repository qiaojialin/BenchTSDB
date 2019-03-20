package cn.edu.thu.database;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBManager implements IDataBaseManager {

    private InfluxDB influxDB;
    private static Logger logger = LoggerFactory.getLogger(InfluxDBManager.class);
    private String measurementId = "station";
    private String database = "test";
    private Config config;

    private static String COUNT_SQL_WITH_TIME = "select count(%s) from %s where time >= %dms and time <= %dms and %s='%s'";

    private static String COUNT_SQL_WITHOUT_TIME = "select count(%s) from %s where %s ='%s'";


    public InfluxDBManager(Config config) {
        this.config = config;
        influxDB = InfluxDBFactory.connect(config.INFLUXDB_URL);
    }

    @Override
    public void createSchema() {
        influxDB.deleteDatabase(database);
        influxDB.createDatabase(database);
    }

    @Override
    public long count(String tagValue, String field, long startTime, long endTime) {

        String sql;

        if(startTime == -1 || endTime == -1) {
            sql = String.format(COUNT_SQL_WITHOUT_TIME, field, measurementId, config.TAG_NAME, tagValue);
        } else {
            sql = String.format(COUNT_SQL_WITH_TIME, field, measurementId, startTime, endTime, config.TAG_NAME, tagValue);
        }

        logger.debug("Executing sql {}", sql);

        long start = System.currentTimeMillis();

        QueryResult queryResult = influxDB.query(new Query(sql, database));

        logger.debug(queryResult.toString());

        return System.currentTimeMillis() - start;

    }

    @Override
    public long flush() {
        return 0;
    }

    @Override
    public long close() {
        return 0;
    }

    @Override
    public long insertBatch(List<Record> records) {

        // get data points
        List<Point> points = convertRecords(records);

        BatchPoints batchPoints = BatchPoints.database(database).points(points.toArray(new Point[0])).build();

        long start = System.currentTimeMillis();

        try {
            influxDB.write(batchPoints);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("Failed to connect to")) {
                logger.error("InfluxDBManager is down!!!!!!");
            } else {
                e.printStackTrace();
            }
        }

        return System.currentTimeMillis() - start;
    }

    private List<Point> convertRecords(List<Record> records) {
        List<Point> points = new ArrayList<>();
        for (Record record : records) {
            Point point = convertRecord(record);
            points.add(point);
        }
        return points;
    }

    private Point convertRecord(Record record) {

        Map<String, String> tagSet = new HashMap<>();
        tagSet.put(config.TAG_NAME, record.tag);

        HashMap<String, Object> fieldSet = new HashMap<>();
        for(int i = 0; i < config.FIELDS.length; i++) {
            fieldSet.put(config.FIELDS[i], record.fields.get(i));
        }

        return Point.measurement(measurementId)
                .time(record.timestamp, TimeUnit.MILLISECONDS)
                .tag(tagSet)
                .fields(fieldSet)
                .build();
    }

}
