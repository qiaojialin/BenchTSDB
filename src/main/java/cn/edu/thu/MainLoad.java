package cn.edu.thu;

import cn.edu.thu.common.BenchmarkExceptionHandler;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.DatabaseFactory;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.writer.RealDatasetWriter;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MainLoad {

  private static Logger logger = LoggerFactory.getLogger(MainLoad.class);

  public static void main(String[] args) {

    if (args == null || args.length == 0) {
      args = new String[]{"conf/config.properties"};
    }

    final Statistics statistics = new Statistics();

    Config config;
    try {
      FileInputStream fileInputStream = new FileInputStream(args[0]);
      config = new Config(fileInputStream);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Load config from {} failed, using default config", args[0]);
      config = new Config();
    }

    // init database
    IDataBaseManager database = DatabaseFactory.getDbManager(config, null);
    database.initServer();

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
    statistics.fileNum.addAndGet(files.size());

    Collections.sort(files);

    Map<String, List<String>> map = new HashMap<>();
    for (String filePath : files) {
      if (filePath.contains(".DS_Store")) {
        continue;
      }
      String deviceId = DatabaseFactory.getDeviceId(config, filePath);
      map.computeIfAbsent(deviceId, k -> new ArrayList<>()).add(filePath);
    }

    Thread.UncaughtExceptionHandler handler = new BenchmarkExceptionHandler();
    ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);
    Config finalConfig = config;
    map.forEach((deviceId, fileList) -> {
      Thread thread = new Thread(new RealDatasetWriter(finalConfig, deviceId, fileList, statistics));
      thread.setUncaughtExceptionHandler(handler);
      executorService.submit(thread);
    });

    executorService.shutdown();
    logger.info("@+++<<<: shutdown thread pool");

    // wait for all threads done
    boolean allDown = false;
    while (!allDown) {
      if (executorService.isTerminated()) {
        allDown = true;
      }
    }

    logger.info("All done! Total records:{}, points:{}, time:{} ms, speed:{} ", statistics.recordNum,
        statistics.pointNum, (float)statistics.timeCost.get() / 1000_1000F, statistics.speed());

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
