package cn.edu.thu.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    // INFLUXDB, OPENTSDB, SUMMARYSTORE
    public String DATABASE = "INFLUXDB";

    public int THREAD_NUM = 2;

    public String INFLUXDB_URL = "http://127.0.0.1:8086";
    public String OPENTSDB_URL = "http://127.0.0.1:4242";

    public String DATA_DIR = "data";

    public String tag1 = "tag1";
    public String tag2 = "tag2";

    public String[] FIELDS = new String[]{"TEMP", "DEWP", "SLP", "STP",
            "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"};



    // for query

    public String QUERY_TAG_1 = "033110";
    public String QUERY_TAG_2 = "99999";

    public String FIELD = "MXSPD";

    //  1072972800000L, -1
    public long START_TIME = 1072972800000L;

    public long END_TIME = -1;


    public Config() {
        Properties properties = new Properties();
        properties.putAll(System.getenv());
        load(properties);
        logger.debug("construct config without config file");
    }

    public Config(InputStream stream) throws IOException {
        Properties properties = new Properties();

        // first from file then from system
        properties.load(stream);
        properties.putAll(System.getenv());
        load(properties);
        logger.debug("construct config with config file");
    }

    private void load(Properties properties) {

        DATABASE = properties.getOrDefault("DATABASE", DATABASE).toString();
        THREAD_NUM = Integer.parseInt(properties.getOrDefault("THREAD_NUM", THREAD_NUM).toString());
        DATA_DIR = properties.getOrDefault("DATA_DIR", DATA_DIR).toString();
        INFLUXDB_URL = properties.getOrDefault("INFLUX_URL", INFLUXDB_URL).toString();
        OPENTSDB_URL = properties.getOrDefault("OPENTSDB_URL", OPENTSDB_URL).toString();

        QUERY_TAG_1 = properties.getOrDefault("QUERY_TAG_1", QUERY_TAG_1).toString();
        QUERY_TAG_2 = properties.getOrDefault("QUERY_TAG_2", QUERY_TAG_2).toString();
        FIELD = properties.getOrDefault("FIELD", FIELD).toString();
        START_TIME = Long.parseLong(properties.getOrDefault("START_TIME", START_TIME).toString());
        END_TIME = Long.parseLong(properties.getOrDefault("END_TIME", END_TIME).toString());

    }
}