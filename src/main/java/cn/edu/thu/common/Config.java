package cn.edu.thu.common;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    // INFLUXDB, OPENTSDB, SUMMARYSTORE, WATERWHEEL, KAIROSDB, TSFILE, PARQUET, ORC
    public String DATABASE = "TSFILE";

    // NOAA, GEOLIFE, MLAB_UTILIZATION, MLAB_IP, TDRIVE, REDD
    public String DATA_SET = "REDD";
    public String DATA_DIR = "data/redd_low";

    // out file path
    public String FILE_PATH = "redd.tsfile";


    public int BEGIN_FILE = 0;
    public int END_FILE = 100000;

    public static final String TAG_NAME = "deviceId";
    public static final String TIME_NAME = "time";
    public static boolean FOR_QUERY = false;

    public int THREAD_NUM = 1;
    public int BATCH_SIZE = 500;

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
    public long START_TIME = 0;
    public long END_TIME = 1946816515000L;

    // noaa
//    public String QUERY_TAG = "010230_99999";
//    public String FIELD = "TEMP";
//    public long START_TIME = 0L;
//    public long END_TIME = 1946816515000L;
    public int NOAA_EXTENDED_COLUMN_NUMBER = 1;

    public static final List<List<List<Boolean>>> NOAA_NULL_VALUES_MAP = new ArrayList<>();

    static {
        NOAA_NULL_VALUES_MAP.add(null); // 0
        NOAA_NULL_VALUES_MAP.add(Arrays.asList(
            Arrays.asList(false, false, false, false, false, false, false, false, false, false,
                false, false, false, false)
        )); // 1
        NOAA_NULL_VALUES_MAP.add(Arrays.asList(
            Arrays.asList(false, false, false, false, false, false, false, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, false, false, false,
                false, false, false, false)
        )); // 2
        NOAA_NULL_VALUES_MAP.add(null); // 3
        NOAA_NULL_VALUES_MAP.add(Arrays.asList(
            Arrays.asList(false, false, false, true, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, false, false, false, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, false, false, false, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, false,
                false, false, false, false)
        )); // 4
        NOAA_NULL_VALUES_MAP.add(null); // 5
        NOAA_NULL_VALUES_MAP.add(null); // 6
        NOAA_NULL_VALUES_MAP.add(Arrays.asList(
            Arrays.asList(false, false, true, true, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, false, false, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, false, false, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, false, false, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, false, false,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, true,
                false, false, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, true,
                true, true, false, false)
        )); // 7
        NOAA_NULL_VALUES_MAP.add(null); // 8
        NOAA_NULL_VALUES_MAP.add(null); // 9
        NOAA_NULL_VALUES_MAP.add(null); // 10
        NOAA_NULL_VALUES_MAP.add(null); // 11
        NOAA_NULL_VALUES_MAP.add(null); // 12
        NOAA_NULL_VALUES_MAP.add(null); // 13
        NOAA_NULL_VALUES_MAP.add(Arrays.asList(
            Arrays.asList(false, true, true, true, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, false, true, true, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, false, true, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, false, true, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, false, true, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, false, true, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, false, true, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, false, true, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, false, true,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, false,
                true, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, true,
                false, true, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, true,
                true, false, true, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, true,
                true, true, false, true),
            Arrays.asList(true, true, true, true, true, true, true, true, true, true,
                true, true, true, false)
        )); // 14
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
                PRECISION = new int[]{6, 6, 0, 0};
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
        NOAA_EXTENDED_COLUMN_NUMBER = Integer.parseInt(properties.getOrDefault("NOAA_EXTENDED_COLUMN_NUMBER", NOAA_EXTENDED_COLUMN_NUMBER).toString());

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