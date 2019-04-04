package cn.edu.thu.extend.horizon;

import cn.edu.thu.extend.record.TDriveRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HExtendRedd implements Runnable {

  private List<String> files;
  private int copyNum;
  private String outputDir;
  private int offset;

  private Logger logger = LoggerFactory.getLogger(HExtendRedd.class);

  public HExtendRedd(List<String> files, int copyNum, String outputDir, int offset) {
    this.files = files;
    this.copyNum = copyNum;
    this.outputDir = outputDir;
    this.offset = offset;
  }

  @Override
  public void run() {
    try {

      // extend each file
      for (int s = 0; s < files.size(); s++) {
        String file = files.get(s);
        logger.info("Thread{} start to extend {} file: {}", Thread.currentThread().getName(), s,
            file);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        Set<TDriveRecord> recordSet = new TreeSet<>();
        String str;

        while ((str = bufferedReader.readLine()) != null) {
          String[] items = str.split(" ");
          long time = Long.parseLong(items[0]);
          TDriveRecord record = new TDriveRecord(time, items[1]);
          recordSet.add(record);
        }
        bufferedReader.close();

        for (int t = 0; t < copyNum; t++) {
          int fileId = (s + offset) * copyNum + t;
          FileWriter writer = new FileWriter(outputDir + fileId + ".txt");
          boolean isFirst = true;
          for (TDriveRecord record : recordSet) {
            if (isFirst) {
              writer.write(record.genRecordStr(" "));
              isFirst = false;
            } else {
              writer.write("\n" + record.genRecordStr(" "));
            }
          }
          writer.close();
          logger.debug("file {} is finished.", fileId);
        }

      }
      logger.info("I'm done.");
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}


