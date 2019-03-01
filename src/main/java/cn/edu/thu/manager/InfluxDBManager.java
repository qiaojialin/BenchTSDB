package cn.edu.thu.manager;

import cn.edu.thu.Record;
import cn.edu.thu.conf.Config;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDBManager implements IDBManager {

    private InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
    private static Logger logger = LoggerFactory.getLogger(InfluxDBManager.class);
    private String measurementId = "default";
    private Config config;

    public InfluxDBManager(Config config) {
        this.config = config;
    }

    @Override
    public void createSchema() {

    }

    @Override
    public void process(List<Record> records) {

        String database = "";

        // get data points
        List<Point> points = convertRecords(records);

        BatchPoints batchPoints = BatchPoints.database(database).points(points.toArray(new Point[0])).build();
        try {
            influxDB.write(batchPoints);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("Failed to connect to")) {
                logger.error("InfluxDBManager is down!!!!!!");
            } else {
                e.printStackTrace();
            }
        }

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

        HashMap<String, String> tagSet = new HashMap<>();
        tagSet.put(config.SERIESID, record.deviceId);

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
