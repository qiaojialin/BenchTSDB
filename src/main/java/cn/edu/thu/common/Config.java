package cn.edu.thu.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Config {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    // INFLUXDB, OPENTSDB, SUMMARYSTORE, WATERWHEEL
    public String DATABASE = "INFLUXDB";

    // NOAA, GEO, RDF
    public String DATA_SET = "RDF";

    public String TAG_NAME = "deviceId";

    public int THREAD_NUM = 1;

    public String INFLUXDB_URL = "http://127.0.0.1:8086";
    public String OPENTSDB_URL = "http://127.0.0.1:4242";
    public String WATERWHEEL_IP = "127.0.0.1";
    public String SUMMARYSTORE_PATH = "sstore";

    // noaa, geolife
    public String DATA_DIR = "data/rdf";
    public int BEGINE_FILE = 0;
    public int END_FILE = 1000000;


    public String[] FIELDS = null;


    // for query

    public Map<String, String> tags = new HashMap<>();

    public String QUERY_TAG = "010230_99999";

    public String FIELD = "MXSPD";

    //  1893484839000L, -1
    public long START_TIME = 1893484839000L;

    public long END_TIME = 1924934439000L;

    private void init() {
        switch (DATA_SET) {
            case "NOAA":
                FIELDS = new String[]{"TEMP", "DEWP", "SLP", "STP",
                        "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"};
                break;
            case "GEO":
                FIELDS = new String[]{"Latitude", "Longitude", "Zero", "Altitude"};
                break;
            case "RDF":
                FIELDS = new String[]{"Latitude", "Longitude", "Zero", "Altitude"};
                break;
            default:
                throw new RuntimeException(DATA_SET + " is not support");
        }
    }

    public Config() {
        Properties properties = new Properties();
        properties.putAll(System.getenv());
        load(properties);
        init();
        logger.debug("construct config without config file");
    }

    public Config(InputStream stream) throws IOException {
        Properties properties = new Properties();

        // first from file then from system
        properties.load(stream);
        properties.putAll(System.getenv());
        load(properties);
        init();
        logger.debug("construct config with config file");
    }


    private void load(Properties properties) {

        DATABASE = properties.getOrDefault("DATABASE", DATABASE).toString();
        DATA_SET = properties.getOrDefault("DATA_SET", DATA_SET).toString();
        THREAD_NUM = Integer.parseInt(properties.getOrDefault("THREAD_NUM", THREAD_NUM).toString());
        DATA_DIR = properties.getOrDefault("DATA_DIR", DATA_DIR).toString();
        INFLUXDB_URL = properties.getOrDefault("INFLUX_URL", INFLUXDB_URL).toString();
        OPENTSDB_URL = properties.getOrDefault("OPENTSDB_URL", OPENTSDB_URL).toString();
        WATERWHEEL_IP = properties.getOrDefault("WATERWHEEL_IP", WATERWHEEL_IP).toString();
        SUMMARYSTORE_PATH = properties.getOrDefault("SUMMARYSTORE_PATH", SUMMARYSTORE_PATH).toString();
        BEGINE_FILE = Integer.parseInt(properties.getOrDefault("BEGINE_FILE", BEGINE_FILE).toString());
        END_FILE = Integer.parseInt(properties.getOrDefault("END_FILE", END_FILE).toString());


        String tag = properties.getOrDefault("QUERY_TAG", QUERY_TAG).toString();
        tags.put("tag", tag);

        FIELD = properties.getOrDefault("FIELD", FIELD).toString();
        START_TIME = Long.parseLong(properties.getOrDefault("START_TIME", START_TIME).toString());
        END_TIME = Long.parseLong(properties.getOrDefault("END_TIME", END_TIME).toString());


    }
}