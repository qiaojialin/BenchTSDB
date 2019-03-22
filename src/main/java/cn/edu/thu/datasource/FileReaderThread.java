package cn.edu.thu.datasource;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import cn.edu.thu.datasource.parser.GeolifeParser;
import cn.edu.thu.datasource.parser.IParser;
import cn.edu.thu.datasource.parser.MLabIPParser;
import cn.edu.thu.datasource.parser.MLabUtilizationParser;
import cn.edu.thu.datasource.parser.NOAAParser;
import cn.edu.thu.datasource.parser.TDriveParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FileReaderThread implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(FileReaderThread.class);
  private IDataBaseManager database;
  private Config config;
  private int threadId;
  private IParser parser;
  private final Statistics statistics;
  private List<String> realFiles;

  public FileReaderThread(IDataBaseManager database, Config config, int threadId,
      List<String> files,
      final Statistics statistics) {
    this.database = database;
    this.config = config;
    this.threadId = threadId;
    this.realFiles = files;
    this.statistics = statistics;

    switch (config.DATA_SET) {
      case "NOAA":
        parser = new NOAAParser();
        break;
      case "GEOLIFE":
        parser = new GeolifeParser();
        break;
      case "TDRIVE":
        parser = new TDriveParser();
        break;
      case "MLAB_IP":
        parser = new MLabIPParser(config);
        break;
      case "MLAB_UTILIZATION":
        parser = new MLabUtilizationParser();
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }

  }

  @Override
  public void run() {

    logger.info("thread running!, need to read {} files", realFiles.size());

    long totalTime = 1;

    try {

      long lineNum = 0;

      List<Record> records = new ArrayList<>();
      // read files
      for (int i = 0; i < realFiles.size(); i++) {

        String filePath = realFiles.get(i);

        records.addAll(parser.parse(filePath));

        if (records.size() < config.BATCH_SIZE) {
          continue;
        }

        // here records size is enough, maybe too large, so split
        // make sure each batch <= config.BATCH_SIZE
        List<List<Record>> batches = new ArrayList<>();
        List<Record> tempBatch = new ArrayList<>();
        for (int j = 0; j < records.size(); j++) {
          tempBatch.add(records.get(j));
          if (tempBatch.size() >= config.BATCH_SIZE) {
            batches.add(tempBatch);
            tempBatch = new ArrayList<>();
          }
        }
        if (!tempBatch.isEmpty()) {
          batches.add(tempBatch);
        }

        // reach a batch
        lineNum += records.size();

        for (List<Record> batch : batches) {
          long timecost = database.insertBatch(batch);
          logger
              .info("write a batch of {} records in {} ms, the {}-th file is down", batch.size(),
                  timecost, i);
          totalTime += timecost;
        }
        records.clear();

      }

      // process the last batch
      if (!records.isEmpty()) {
        lineNum += records.size();
        long timecost = database.insertBatch(records);
        logger
            .info("Write the last batch of {} records in {} ms, {} files down!", records.size(),
                timecost, realFiles.size());
        totalTime += timecost;
      }

      totalTime += database.flush();

      statistics.timeCost.addAndGet(totalTime);
      statistics.lineNum.addAndGet(lineNum);
      statistics.pointNum.addAndGet(lineNum * config.FIELDS.length);

    } catch (Exception e) {
      e.printStackTrace();
    }


  }

}
