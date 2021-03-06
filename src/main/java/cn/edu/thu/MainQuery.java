package cn.edu.thu;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

public class MainQuery {

    private static Logger logger = LoggerFactory.getLogger(MainQuery.class);

    public static void main(String[] args) {

        if (args.length == 0) {
            args = new String[]{"conf/config.properties"};
        }

        Config config = null;
        try {
            FileInputStream fileInputStream = new FileInputStream(args[0]);
            config = new Config(fileInputStream);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Load config from {} failed, using default config", args[0]);
            System.exit(1);
        }

        Config.FOR_QUERY = true;

        IDataBaseManager database = DatabaseFactory.getDbManager(config);
        database.initClient();

        long start = System.nanoTime();
        database.count(config.QUERY_TAG, config.FIELD, config.START_TIME, config.END_TIME);
        start = System.nanoTime()-start;
        logger.info("query time: {} ms", (float)start / 1000_000F);

    }
}
