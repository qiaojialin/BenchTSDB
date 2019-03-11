package cn.edu.thu.client;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.manager.IDataBase;
import cn.edu.thu.manager.InfluxDB;
import cn.edu.thu.manager.OpenTSDB;
import cn.edu.thu.manager.WaterWheel;
import cn.edu.thu.parser.IParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class ClientThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ClientThread.class);
    private IDataBase database;
    private Config config;
    private int threadId;
    private IParser parser;


    public ClientThread(IParser parser, Config config, int threadId) {
        switch (config.DATABASE) {
            case "INFLUXDB":
                database = new InfluxDB(config);
                break;
            case "OPENTSDB":
                database = new OpenTSDB(config);
                break;
            case "WATERWHEEL":
                database = new WaterWheel(config);
                break;
            default:
                throw new RuntimeException(config.DATABASE + " not supported");
        }
        this.parser = parser;
        this.config = config;
        this.threadId = threadId;
    }

    @Override
    public void run() {

        //read file

        long totalTime = 1;
        long totalPoints = 0;

        try {
            File dirFile = new File(config.DATA_DIR);
            if (!dirFile.exists()) {
                logger.error(config.DATA_DIR + " do not exit");
                return;
            }

            List<String> files = new ArrayList<>();
            getAllFiles(config.DATA_DIR, files);

            Collections.sort(files);

            long lineNum = 0;
            int fileNum = 0;

            // datafile name begins from 0

            logger.info("total file num: {}", files.size());

            for (int i = 0; i < files.size(); i++) {

                if(i< config.BEGINE_FILE || i > config.END_FILE) {
                    continue;
                }

                String filePath = files.get(i);

                // only read the file that can be exacted division by threadid
                if (i % config.THREAD_NUM != threadId) {
                    continue;
                }

                fileNum++;

                List<Record> records = parser.parse(filePath);

                lineNum += records.size();

                // write all data in this file to database
                long timecost = database.insertBatch(records);

                totalTime += timecost;

                logger.debug("processed the {}-th file in : {} ms", i, timecost);
            }

            totalTime += database.close();

            logger.info("total produce {} files and {} lines", fileNum, lineNum);

            // points per second
            totalPoints = lineNum * config.FIELDS.length;
            long speed = totalPoints / totalTime * 1000;

            logger.info("points:{},time:{},ms,speed:{},points/s", totalPoints, totalTime, speed);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private void getAllFiles(String strPath, List<String> files) {
        File f = new File(strPath);
        if (f.isDirectory()) {
            File[] fs = f.listFiles();
            for (File f1 : fs) {
                String fsPath = f1.getAbsolutePath();
                getAllFiles(fsPath, files);
            }
        } else if (f.isFile()) {
            files.add(f.getAbsolutePath());
        }
    }



}
