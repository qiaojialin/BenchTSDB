package cn.edu.thu;

import cn.edu.thu.database.kairosdb.KairosDBManager;
import cn.edu.thu.database.test.NullManager;
import cn.edu.thu.database.waterwheel.WaterWheelManager;
import cn.edu.thu.datasource.CalculateThread;
import cn.edu.thu.datasource.FileReaderThread;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import cn.edu.thu.database.opentsdb.OpenTSDBManager;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    IDataBaseManager database;
    switch (config.DATABASE) {
      case "NULL":
        database = new NullManager();
        break;
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
        database = new SummaryStoreManager(config, false);
        break;
      case "WATERWHEEL":
        database = new WaterWheelManager(config, false);
        break;
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }
    database.createSchema();

    logger.info("thread num : {}", config.THREAD_NUM);
    logger.info("using database: {}", config.DATABASE);

    File dirFile = new File(config.DATA_DIR);
    if (!dirFile.exists()) {
      logger.error(config.DATA_DIR + " do not exit");
      return;
    }

    List<String> files = new ArrayList<>();
    getAllFiles(config.DATA_DIR, files);
    logger.info("total files: {}", files.size());
    statistics.fileNum = files.size();

    Collections.sort(files);

    List<List<String>> thread_files = new ArrayList<>();
    for (int i = 0; i < config.THREAD_NUM; i++) {
      thread_files.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {

//      if (i < config.BEGINE_FILE || i > config.END_FILE) {
//        continue;
//      }

      String filePath = files.get(i);
      if (filePath.contains(".DS_Store")) {
        continue;
      }
      int thread = i % config.THREAD_NUM;
      thread_files.get(thread).add(filePath);
    }


    ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);
    for (int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
      executorService.submit(
          new CalculateThread(database, config, threadId, thread_files.get(threadId), statistics));
    }

    executorService.shutdown();
    logger.info("@+++<<<: shutdown thread pool");

    // wait for all threads done
    boolean allDown = false;
    while (!allDown) {
      if (executorService.isTerminated()) {
        allDown = true;
      }
    }

    database.close();

    logger.info("All done! Total lines:{}, points:{}, time:{}ms, speed:{} ", statistics.lineNum,
        statistics.pointNum,
        statistics.timeCost, statistics.speed());

  }

  private static void getAllFiles(String strPath, List<String> files) {
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
