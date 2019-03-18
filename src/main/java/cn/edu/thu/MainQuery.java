package cn.edu.thu;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.*;
import cn.edu.thu.database.kairosdb.KairosDBM;
import cn.edu.thu.database.opentsdb.OpenTSDBM;
import cn.edu.thu.database.waterwheel.WaterWheelM;
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

        IDataBaseM database = null;
        switch (config.DATABASE) {
            case "INFLUXDB":
                database = new InfluxDBM(config);
                break;
            case "OPENTSDB":
                database = new OpenTSDBM(config);
                break;
            case "KAIROSDB":
                database = new KairosDBM(config);
                break;
            case "SUMMARYSTORE":
                database = new SummaryStoreM(config, true);
                break;
            case "WATERWHEEL":
                database = new WaterWheelM(config, true);
                break;
            default:
                throw new RuntimeException(config.DATABASE + " not supported");
        }



        long start = System.currentTimeMillis();
//        database.count(config.QUERY_TAG, config.FIELD, config.START_TIME, config.END_TIME);
        database.count(config.QUERY_TAG, config.FIELD, Long.MIN_VALUE, Long.MAX_VALUE);
        start = System.currentTimeMillis()-start;
        logger.info("query time: {}",start);

    }
}
