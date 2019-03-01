package cn.edu.thu.conf;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    public String DATABASE = "SUMMARYSTORE";

    public int THREAD_NUM = 1;

    public String DATA_DIR = "data";

    public String SERIESID = "deviceId";

    public String[] FIELDS = new String[]{"WBAN", "TEMP", "DEWP", "SLP", "STP",
            "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"};


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
    }
}