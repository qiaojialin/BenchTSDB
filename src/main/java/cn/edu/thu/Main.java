package cn.edu.thu;

import cn.edu.thu.client.ClientThread;
import cn.edu.thu.conf.Config;
import cn.edu.thu.manager.IDBManager;
import cn.edu.thu.manager.InfluxDBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String args[]) {

        args = new String[]{"conf/config.properties"};

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

        IDBManager database = new InfluxDBManager(config);

        logger.info("thread num : {}", config.THREAD_NUM);

        for (int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
            executorService.submit(new ClientThread(database, config, threadId));
        }

        executorService.shutdown();

    }
}
