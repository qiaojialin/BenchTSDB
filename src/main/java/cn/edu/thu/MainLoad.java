package cn.edu.thu;

import cn.edu.thu.client.ClientThread;
import cn.edu.thu.common.Config;
import cn.edu.thu.manager.IDataBase;
import cn.edu.thu.manager.InfluxDB;
import cn.edu.thu.manager.OpenTSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MainLoad {

    private static Logger logger = LoggerFactory.getLogger(MainLoad.class);

    public static void main(String args[]) {

        //args = new String[]{"conf/config.properties"};

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

        ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);

        IDataBase database = null;
        switch (config.DATABASE) {
            case "INFLUXDB":
                database = new InfluxDB(config);
                break;
            case "OPENTSDB":
                database = new OpenTSDB(config);
                break;
            case "SUMMARYSTORE":
                break;
            default:
                throw new RuntimeException(config.DATABASE + " not supported");
        }

        logger.info("thread num : {}", config.THREAD_NUM);
        logger.info("using database: {}", config.DATABASE);

        for (int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
            executorService.submit(new ClientThread(database, config, threadId));
        }

        executorService.shutdown();

    }
}
