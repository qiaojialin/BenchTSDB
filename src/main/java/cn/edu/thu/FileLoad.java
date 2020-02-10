package cn.edu.thu;

import backup.MLabUtilizationReader;
import cn.edu.thu.common.BenchmarkExceptionHandler;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.common.Utils;
import cn.edu.thu.database.DatabaseFactory;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.reader.BasicReader;
import cn.edu.thu.reader.GeolifeReader;
import cn.edu.thu.reader.NOAAReader;
import cn.edu.thu.reader.ReddReader;
import cn.edu.thu.reader.TDriveReader;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * write each file along the raw file
 */
public class FileLoad {

  private static Logger logger = LoggerFactory.getLogger(FileLoad.class);

  // if there's args, inputdir, dataset(noaa), database(tsfile)
  public static void main(String[] args) throws IOException {

    //args = new String[]{"conf/config.properties"};

    final Statistics statistics = new Statistics();

    Config config;
    if (args.length > 0) {
      try {
//        FileInputStream fileInputStream = new FileInputStream(args[0]);
//        config = new Config(fileInputStream);
        config = new Config(args[2], args[1], args[0]);

        // initialize the output dir
        File f = new File(config.OUTPUT);
        if(!f.exists()){
          f.mkdir();
        }

      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Load config from {} failed, using default config", args[0]);
        config = new Config();
      }
    } else {
      config = new Config();
    }

    logger.info("thread num : {}", config.THREAD_NUM);
    logger.info("using database: {}", config.DATABASE);

    File dirFile = new File(config.DATA_DIR);
    if (!dirFile.exists()) {
      logger.error(config.DATA_DIR + " do not exit");
      return;
    }
    List<String> files = new ArrayList<>();
    Utils.getAllFiles(config.DATA_DIR, files);
    logger.info("total files: {}", files.size());
    statistics.fileNum.addAndGet(files.size());

    Collections.sort(files);

    List<List<String>> thread_files = new ArrayList<>();
    for (int i = 0; i < config.THREAD_NUM; i++) {
      thread_files.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      String filePath = files.get(i);
      if (filePath.contains(".DS_Store") || filePath.contains(".orc") || filePath.contains(".parquet") || filePath.contains(".tsfile")) {
        continue;
      }
      int thread = i % config.THREAD_NUM;
      thread_files.get(thread).add(filePath);
    }

    long start = System.currentTimeMillis();

    Thread.UncaughtExceptionHandler handler = new BenchmarkExceptionHandler();
    ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);
    for (int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
      Thread thread = new Thread(new Worker(config, thread_files.get(threadId), statistics));
      thread.setUncaughtExceptionHandler(handler);
      executorService.submit(thread);
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

    start = System.currentTimeMillis() - start;

    logger.info("All done! Total records:{}, points:{}, time:{} ms, speed:{} points/s ", statistics.recordNum,
        statistics.pointNum, (float)statistics.timeCost.get() / 1000_000F, statistics.speed());

    logger.info("total time: {}ms, speed: {} points/s", start, statistics.pointNum.get()/start * 1000);
    File f = new File("rpt.txt");
    if(!f.exists()){
      f.createNewFile();
    }
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, true)));
    writer.write(config.DATA_SET + ", " + config.DATABASE + ", " + statistics.speed() + "\n");
    writer.close();
  }

  static class Worker implements Runnable {

    private Logger logger = LoggerFactory.getLogger(Worker.class);
    private List<String> files;
    private Config config;
    private Statistics statistics;

    public Worker(Config config, List<String> files, final Statistics statistics) {
      this.config = config;
      this.files = files;
      this.statistics = statistics;
    }

    @Override
    public void run() {
      try {

        // put each input files into one file
        String outputPath = config.OUTPUT + "/" + config.DATA_SET.toLowerCase() + "." + config.DATABASE.toLowerCase();
        File f = new File(outputPath);
        if(f.exists()){
          f.delete();
        }
        IDataBaseManager database = DatabaseFactory.getFileManager(config, outputPath);
        BasicReader reader;

        database.initClient();
        switch (config.DATA_SET) {
          case "NOAA":
            reader = new NOAAReader(config, files);
            break;
          case "GEOLIFE":
            reader = new GeolifeReader(config, files);
            break;
          case "TDRIVE":
            reader = new TDriveReader(config, files);
            break;
          case "MLAB_UTILIZATION":
            reader = new MLabUtilizationReader(config, files);
            break;
          case "REDD":
            reader = new ReddReader(config, files);
            break;
          default:
            throw new RuntimeException(config.DATA_SET + " not supported");
        }

        long totalTime = 0;
        long recordNum = 0;
        while(reader.hasNextBatch()) {
          List<Record> batch = reader.nextBatch();
          if (batch.size() == 0){
            continue;
          }
          totalTime += database.insertBatch(batch);
          recordNum += batch.size();
        }
        totalTime += database.flush();
        totalTime += database.close();
        statistics.timeCost.addAndGet(totalTime);
        statistics.recordNum.addAndGet(recordNum);
        statistics.pointNum.addAndGet(recordNum * config.FIELDS.length);


        logger.info("I'm done.");
      } catch (Exception e) {
        e.printStackTrace();
      }

    }

//    @Override
//    public void run() {
//      try {
//
//        // extend each file
//        for (String file : files) {
//
//          String outPath = file + "." + config.DATABASE.toLowerCase();
//          IDataBaseManager database = DatabaseFactory.getFileManager(config, outPath);
//          database.initClient();
//
//          logger.info("start to read file: {}", file);
//
//          BasicReader reader;
//          List<String> afile = new ArrayList<>();
//          afile.add(file);
//
//          switch (config.DATA_SET) {
//            case "NOAA":
//              reader = new NOAAReader(config, afile);
//              break;
//            case "GEOLIFE":
//              reader = new GeolifeReader(config, afile);
//              break;
//            case "TDRIVE":
//              reader = new TDriveReader(config, afile);
//              break;
//            case "MLAB_UTILIZATION":
//              reader = new MLabUtilizationReader(config, afile);
//              break;
//            case "REDD":
//              reader = new ReddReader(config, afile);
//              break;
//            default:
//              throw new RuntimeException(config.DATA_SET + " not supported");
//          }
//
//
//          long totalTime = 1;
//          long recordNum = 0;
//
//          while(reader.hasNextBatch()) {
//            List<Record> batch = reader.nextBatch();
//            totalTime += database.insertBatch(batch);
//            recordNum += batch.size();
//          }
//
//          totalTime += database.flush();
//          totalTime += database.close();
//
//          statistics.timeCost.addAndGet(totalTime);
//          statistics.recordNum.addAndGet(recordNum);
//          statistics.pointNum.addAndGet(recordNum * config.FIELDS.length);
//
//        }
//        logger.info("I'm done.");
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//
//    }

  }





}
