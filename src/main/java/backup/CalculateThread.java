package backup;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.IDataBaseManager;
import java.io.File;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateThread implements Runnable{

  private static Logger logger = LoggerFactory.getLogger(CalculateThread.class);
  private IDataBaseManager database;
  private Config config;
  private int threadId;
  private CalculateMLabIPParser parser;
  private final Statistics statistics;
  private List<String> realFiles;

  public CalculateThread(IDataBaseManager database, Config config, int threadId,
      List<String> files,
      final Statistics statistics) {
    this.database = database;
    this.config = config;
    this.threadId = threadId;
    this.realFiles = files;
    this.statistics = statistics;

    switch (config.DATA_SET) {
      case "MLAB_IP":
        parser = new CalculateMLabIPParser(config);
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

      // read files
      for (int i = 0; i < realFiles.size(); i++) {

        String filePath = realFiles.get(i);

        lineNum += parser.parse(filePath);

        logger.info("file: {}-th", i);
      }

      statistics.timeCost.addAndGet(totalTime);
      statistics.recordNum.addAndGet(lineNum);
      statistics.pointNum.addAndGet(lineNum * config.FIELDS.length);

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
