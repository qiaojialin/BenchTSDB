package cn.edu.thu;

import cn.edu.thu.client.ClientThread;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.manager.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.concurrent.*;


public class MainLoad {

    private static Logger logger = LoggerFactory.getLogger(MainLoad.class);

    public static void main(String args[]) {

        //args = new String[]{"conf/config.properties"};

        final Statistics statistics = new Statistics();

        Config config;
        if(args.length > 0) {
            try{
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

        IDataBase database;
        switch (config.DATABASE) {
            case "INFLUXDB":
                database = new InfluxDB(config);
                break;
            case "OPENTSDB":
                database = new OpenTSDB(config);
                break;
            case "SUMMARYSTORE":
                database = new SummaryStoreM(config, false);
                break;
            case "WATERWHEEL":
                database = new WaterWheel(config);
                break;
            default:
                throw new RuntimeException(config.DATABASE + " not supported");
        }
        database.createSchema();

        logger.info("thread num : {}", config.THREAD_NUM);
        logger.info("using database: {}", config.DATABASE);

        ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);
        for(int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
            executorService.submit(new ClientThread(database, config, threadId, statistics));
        }

        executorService.shutdown();
        logger.info("@+++<<<: shutdown thread pool");

        // wait for all threads done
        boolean allDown = false;
        while (!allDown) {
            if(executorService.isTerminated()) {
                allDown = true;
            }
        }

        database.close();

        logger.info("All done! Total lines:{}, points:{}, time:{}ms, speed:{} ", statistics.lineNum, statistics.pointNum,
                statistics.timeCost, statistics.speed());

    }

}
