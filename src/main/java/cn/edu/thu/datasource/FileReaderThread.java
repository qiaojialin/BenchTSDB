package cn.edu.thu.datasource;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import cn.edu.thu.datasource.parser.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class FileReaderThread implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(FileReaderThread.class);
  private IDataBaseManager database;
  private Config config;
  private BasicParser parser;
  private final Statistics statistics;

  public FileReaderThread(IDataBaseManager database, Config config,
      List<String> files,
      final Statistics statistics) {
    this.database = database;
    this.config = config;
    this.statistics = statistics;

    logger.info("thread construct!, need to read {} files", files.size());

    switch (config.DATA_SET) {
      case "NOAA":
        parser = new NOAAParser(config, files);
        break;
      case "GEOLIFE":
        parser = new GeolifeParser(config, files);
        break;
      case "TDRIVE":
        parser = new TDriveParser(config, files);
        break;
      case "MLAB_IP":
        parser = new MLabIPParser(config, files);
        break;
      case "MLAB_UTILIZATION":
        parser = new MLabUtilizationParser(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }

  }

  @Override
  public void run() {

    long totalTime = 1;

    try {

      long recordNum = 0;

      while(parser.hasNextBatch()) {
        List<Record> rowBatch = parser.nextBatch();
        recordNum += rowBatch.size();

        // rowBatch maybe too large, make sure each batch <= config.BATCH_SIZE
        List<List<Record>> batches = new ArrayList<>();
        List<Record> tempBatch = new ArrayList<>();
        for (int j = 0; j < rowBatch.size(); j++) {
          tempBatch.add(rowBatch.get(j));
          if (tempBatch.size() >= config.BATCH_SIZE) {
            batches.add(tempBatch);
            tempBatch = new ArrayList<>();
          }
        }
        if (!tempBatch.isEmpty()) {
          batches.add(tempBatch);
        }

        for (List<Record> batch : batches) {
          long timecost = database.insertBatch(batch);
          logger
              .info("write a batch of {} records in {} ms", batch.size(), timecost);
          totalTime += timecost;
        }

      }

      totalTime += database.flush();

      statistics.timeCost.addAndGet(totalTime);
      statistics.recordNum.addAndGet(recordNum);
      statistics.pointNum.addAndGet(recordNum * config.FIELDS.length);

    } catch (Exception e) {
      e.printStackTrace();
    }


  }

}
