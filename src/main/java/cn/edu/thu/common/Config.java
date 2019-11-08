package cn.edu.thu.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    // INFLUXDB, OPENTSDB, SUMMARYSTORE, WATERWHEEL, KAIROSDB, TSFILE, PARQUET, ORC
    public String DATABASE = "PARQUET";

    // NOAA, GEOLIFE, MLAB_UTILIZATION, MLAB_IP, TDRIVE, REDD
    public String DATA_SET = "NOAA";
    public String DATA_DIR = "data\\noaa";

    // for read
//    public String FILE_PATH = "data/redd_low/house_1/96.txt.orc";

    public String FILE_PATH = "data\\noaa\\501360-99999-1973.op";

    public int BEGIN_FILE = 0;
    public int END_FILE = 100000;

    public static final String TAG_NAME = "deviceId";
    public static final String TIME_NAME = "time";
    public static boolean FOR_QUERY = false;

    public int THREAD_NUM = 1;
    public int BATCH_SIZE = 5000;

    public String INFLUXDB_URL = "http://127.0.0.1:8086";

    public String OPENTSDB_URL = "http://127.0.0.1:4242";
//    public String OPENTSDB_URL = "http://192.168.10.64:4242";

//    public String KAIROSDB_URL = "http://127.0.0.1:1408";
//    public String KAIROSDB_URL = "http://192.168.10.64:1408";
    public String KAIROSDB_URL = "http://192.168.10.66:8080";

    public String SUMMARYSTORE_PATH = "sstore";

    public String WATERWHEEL_IP = "127.0.0.1";
    public String HDFS_IP="hdfs://127.0.0.1:9000/"; // must end with '/'
    public boolean LOCAL = false;

    public String[] FIELDS = null;
    public int[] PRECISION = null;

    public int WATERWHEEL_INGEST_PORT = 10000;
    public int WATERWHEEL_QUERY_PORT = 10001;

    // for query

    // geolife
//    public String QUERY_TAG = "000";
//    public String FIELD = "Latitude";
//    public long START_TIME = 0;
//    public long END_TIME = 1946816515000L;

    // redd
    public String QUERY_TAG = "house_1_channel_1";
    public String FIELD = "value";
    public long START_TIME = 1303277621000L;
    public long END_TIME = 1305109759000L;

    // tdrive
//    public String QUERY_TAG = "1";
//    public String FIELD = "longitude";
//    public long START_TIME = 0;
//    public long END_TIME = Long.MAX_VALUE;

    // noaa
//    public String QUERY_TAG = "010230_99999";
//    public String FIELD = "TEMP";
//    public long START_TIME = 0L;
//    public long END_TIME = 1946816515000L;

    public Config() {
        Properties properties = new Properties();
        properties.putAll(System.getenv());
        load(properties);
        init();
        logger.debug("construct config without config file");
    }

    public Config(InputStream stream) throws IOException {
        Properties properties = new Properties();
        properties.putAll(System.getenv());
        properties.load(stream);
        load(properties);
        init();
        logger.debug("construct config with config file");
    }

    private void init() {
        switch (DATA_SET) {
            case "NOAA":
                FIELDS = new String[]{"TEMP", "DEWP", "SLP", "STP",
                    "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"};
                PRECISION = new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 0};
                break;
            case "GEOLIFE":
                FIELDS = new String[]{"Latitude", "Longitude", "Zero", "Altitude"};
                PRECISION = new int[]{6, 6, 0, 12};
                break;
            case "TDRIVE":
                FIELDS = new String[]{"longitude", "latitude"};
                PRECISION = new int[]{5, 5};
                break;
            case "MLAB_IP":
                FIELDS = new String[]{"connect_time"};
                PRECISION = new int[]{6};
                break;
            case "MLAB_UTILIZATION":
                FIELDS = new String[]{"value"};
                PRECISION = new int[]{10};
                break;
            case "REDD":
                FIELDS = new String[]{"value"};
                PRECISION = new int[]{2};
                break;
            default:
                throw new RuntimeException(DATA_SET + " is not support");
        }
        if (!DATA_DIR.endsWith("/")) {
            DATA_DIR += "/";
        }
        BATCH_SIZE = BATCH_SIZE / FIELDS.length;
        logger.info("use dataset: {}", DATA_SET);
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
        HDFS_IP = properties.getOrDefault("HDFS_IP", HDFS_IP).toString();
        SUMMARYSTORE_PATH = properties.getOrDefault("SUMMARYSTORE_PATH", SUMMARYSTORE_PATH).toString();
        FILE_PATH = properties.getOrDefault("FILE_PATH", FILE_PATH).toString();

        BEGIN_FILE = Integer.parseInt(properties.getOrDefault("BEGIN_FILE", BEGIN_FILE).toString());
        END_FILE = Integer.parseInt(properties.getOrDefault("END_FILE", END_FILE).toString());
        BATCH_SIZE = Integer.parseInt(properties.getOrDefault("BATCH_SIZE", BATCH_SIZE).toString());
        WATERWHEEL_INGEST_PORT = Integer.parseInt(properties.getOrDefault("WATERWHEEL_INGEST_PORT", WATERWHEEL_INGEST_PORT).toString());
        WATERWHEEL_QUERY_PORT = Integer.parseInt(properties.getOrDefault("WATERWHEEL_QUERY_PORT", WATERWHEEL_QUERY_PORT).toString());
        LOCAL = Boolean.parseBoolean(properties.getOrDefault("LOCAL", LOCAL).toString());

        QUERY_TAG = properties.getOrDefault("QUERY_TAG", QUERY_TAG).toString();

        FIELD = properties.getOrDefault("FIELD", FIELD).toString();

        String startTime = properties.getOrDefault("START_TIME", START_TIME).toString();
        if(startTime.toLowerCase().contains("min")) {
            START_TIME = Long.MIN_VALUE;
        } else {
            START_TIME = Long.parseLong(startTime);
        }

        String endTime = properties.getOrDefault("END_TIME", END_TIME).toString();
        if(endTime.toLowerCase().contains("max")) {
            END_TIME = Long.MAX_VALUE;
        } else {
            END_TIME = Long.parseLong(endTime);
        }

    }
}