package cn.edu.thu;

import cn.edu.thu.common.Config;
import cn.edu.thu.manager.IDataBase;
import cn.edu.thu.manager.InfluxDB;
import cn.edu.thu.manager.OpenTSDB;
import cn.edu.thu.manager.SummaryStoreM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

public class MainQuery {

    private static Logger logger = LoggerFactory.getLogger(MainQuery.class);

    public static void main(String[] args) {

        Config config;
        if (args.length > 0) {
            try {
                FileInputStream fileInputStream = new FileInputStream(args[0]);
                config = new Config(fileInputStream);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Load config from {} failed, using default config", args[0]);
                config = new Config();
            }
        } else {
            config = new Config();
        }

        IDataBase database = null;
        switch (config.DATABASE) {
            case "INFLUXDB":
                database = new InfluxDB(config);
                break;
            case "OPENTSDB":
                database = new OpenTSDB(config);
                break;
            case "SUMMARYSTORE":
                database = new SummaryStoreM(config, true);
                break;
            default:
                throw new RuntimeException(config.DATABASE + " not supported");
        }

        long start = System.currentTimeMillis();
        database.count(config.QUERY_TAG_1, config.QUERY_TAG_2, config.FIELD, config.START_TIME, config.END_TIME);
        start = System.currentTimeMillis()-start;
        logger.info("query time: {}",start);

    }
}
