package cn.edu.thu.writer;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.*;
import cn.edu.thu.reader.BasicReader;
import cn.edu.thu.reader.GeolifeReader;
import cn.edu.thu.reader.NOAAReader;
import cn.edu.thu.reader.ReddReader;
import cn.edu.thu.reader.TDriveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import backup.MLabUtilizationReader;

public class RealDatasetWriter implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(RealDatasetWriter.class);
  private IDataBaseManager database;
  private Config config;
  private BasicReader reader;
  private final Statistics statistics;

  public RealDatasetWriter(Config config, List<String> files, final Statistics statistics) {
    this.database = DatabaseFactory.getDbManager(config);
    database.initClient();
    this.config = config;
    this.statistics = statistics;

    logger.info("thread construct!, need to read {} files", files.size());

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

  }

  @Override
  public void run() {

    long totalTime = 1;

    try {

      long recordNum = 0;

      while(reader.hasNextBatch()) {
        List<Record> batch = reader.nextBatch();
        totalTime += database.insertBatch(batch);
        recordNum += batch.size();
      }

      totalTime += database.flush();
      totalTime += database.close();

      statistics.timeCost.addAndGet(totalTime);
      statistics.recordNum.addAndGet(recordNum);
      statistics.pointNum.addAndGet(recordNum * config.FIELDS.length);

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
