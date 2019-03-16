package cn.edu.thu.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    // INFLUXDB, OPENTSDB, SUMMARYSTORE, WATERWHEEL, KAIROSDB
    public String DATABASE = "INFLUXDB";

    // NOAA, GEOLIFE, MLAB_UTILIZATION, MLAB_IP, TDRIVE
    public String DATA_SET = "MLAB_IP";

    public String TAG_NAME = "deviceId";

    public int THREAD_NUM = 1;

    public String INFLUXDB_URL = "http://127.0.0.1:8086";
    public String OPENTSDB_URL = "http://127.0.0.1:4242";
    public String KAIROSDB_URL = "http://127.0.0.1:8080";
    public String SUMMARYSTORE_PATH = "sstore";

    public String WATERWHEEL_IP = "127.0.0.1";
    public boolean LOCAL = true;

    // noaa, geolife, mlab_utilization, mlab_ip, tdrive
    public String DATA_DIR = "data/mlab_ip";
    public int BEGINE_FILE = 0;
    public int END_FILE = 100000;

    public String[] FIELDS = null;

    public int WATERWHEEL_INGEST_PORT = 10000;
    public int WATERWHEEL_QUERY_PORT = 10001;

    // for query
    public String QUERY_TAG = "000";

    public String FIELD = "Latitude";

    //  1893484839000L, -1
    public long START_TIME = -1;

    public long END_TIME = 1924934439000L;

    private void init() {
        switch (DATA_SET) {
            case "NOAA":
                FIELDS = new String[]{"TEMP", "DEWP", "SLP", "STP",
                        "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"};
                break;
            case "GEOLIFE":
                FIELDS = new String[]{"Latitude", "Longitude", "Zero", "Altitude"};
                break;
            case "TDRIVE":
                FIELDS = new String[]{"longitude", "latitude"};
                break;
            case "MLAB_IP":
                FIELDS = new String[]{"download_speed", "neubot_version", "connect_time", "upload_speed"};
                break;
            case "MLAB_UTILIZATION":
                FIELDS = new String[]{"value"};
                break;
            default:
                throw new RuntimeException(DATA_SET + " is not support");
        }
        logger.info("use dataset: {}", DATA_SET);
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
        KAIROSDB_URL = properties.getOrDefault("KAIROSDB_URL", KAIROSDB_URL).toString();
        WATERWHEEL_IP = properties.getOrDefault("WATERWHEEL_IP", WATERWHEEL_IP).toString();
        SUMMARYSTORE_PATH = properties.getOrDefault("SUMMARYSTORE_PATH", SUMMARYSTORE_PATH).toString();
        BEGINE_FILE = Integer.parseInt(properties.getOrDefault("BEGINE_FILE", BEGINE_FILE).toString());
        END_FILE = Integer.parseInt(properties.getOrDefault("END_FILE", END_FILE).toString());
        WATERWHEEL_INGEST_PORT = Integer.parseInt(properties.getOrDefault("WATERWHEEL_INGEST_PORT", WATERWHEEL_INGEST_PORT).toString());
        WATERWHEEL_QUERY_PORT = Integer.parseInt(properties.getOrDefault("WATERWHEEL_QUERY_PORT", WATERWHEEL_QUERY_PORT).toString());
        LOCAL = Boolean.parseBoolean(properties.getOrDefault("LOCAL", LOCAL).toString());

        QUERY_TAG = properties.getOrDefault("QUERY_TAG", QUERY_TAG).toString();

        FIELD = properties.getOrDefault("FIELD", FIELD).toString();
        START_TIME = Long.parseLong(properties.getOrDefault("START_TIME", START_TIME).toString());
        END_TIME = Long.parseLong(properties.getOrDefault("END_TIME", END_TIME).toString());


    }
}