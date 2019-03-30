package cn.edu.thu;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.*;
import cn.edu.thu.database.fileformat.ORCManager;
import cn.edu.thu.database.fileformat.ParquetManager;
import cn.edu.thu.database.fileformat.TsFileManager;
import cn.edu.thu.database.kairosdb.KairosDBManager;
import cn.edu.thu.database.opentsdb.OpenTSDBManager;
import cn.edu.thu.database.waterwheel.WaterWheelManager;
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

        IDataBaseManager database = null;
        switch (config.DATABASE) {
            case "INFLUXDB":
                database = new InfluxDBManager(config);
                break;
            case "OPENTSDB":
                database = new OpenTSDBManager(config);
                break;
            case "KAIROSDB":
                database = new KairosDBManager(config);
                break;
            case "SUMMARYSTORE":
                database = new SummaryStoreManager(config, true);
                break;
            case "WATERWHEEL":
                database = new WaterWheelManager(config, true);
                break;
            case "TSFILE":
                database = new TsFileManager(config);
                break;
            case "PARQUET":
                database = new ParquetManager(config);
                break;
            case "ORC":
                database = new ORCManager(config);
                break;
            default:
                throw new RuntimeException(config.DATABASE + " not supported");
        }



        long start = System.nanoTime();
        database.count(config.QUERY_TAG, config.FIELD, config.START_TIME, config.END_TIME);
        start = System.nanoTime()-start;
        logger.info("query time: {} ms", (float)start / 1000_000F);

    }
}
