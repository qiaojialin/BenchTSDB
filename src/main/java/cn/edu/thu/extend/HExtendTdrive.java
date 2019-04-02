package cn.edu.thu.extend;

import cn.edu.thu.extend.record.TDriveRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HExtendTdrive implements Runnable {

  private List<String> files;
  private int copyNum;
  private String outputDir;
  private int offset;

  private Logger logger = LoggerFactory.getLogger(HExtendTdrive.class);
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

  public HExtendTdrive(List<String> files, int copyNum, String outputDir, int offset) {
    this.files = files;
    this.copyNum = copyNum;
    this.outputDir = outputDir;
    this.offset = offset;
  }

  @Override
  public void run() {
    try {
      dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

      // extend each file
      for (int s = 0; s < files.size(); s++) {
        String file = files.get(s);
        logger.info("Thread{} start to extend {} file: {}", Thread.currentThread().getName(), s,
            file);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        Set<TDriveRecord> recordSet = new TreeSet<>();
        String str;

        while ((str = bufferedReader.readLine()) != null) {
          String[] items = str.split(",");
          Date date = dateFormat.parse(items[1]);
          TDriveRecord record = new TDriveRecord(date.getTime(), items[2], items[3]);
          recordSet.add(record);
        }
        bufferedReader.close();

        for (int t = 0; t < copyNum; t++) {
          int fileId = (s + offset) * copyNum + t;
          FileWriter writer = new FileWriter(outputDir + fileId + ".txt");
          boolean isFirst = true;
          for (TDriveRecord record : recordSet) {
            if (isFirst) {
              writer.write(fileId + "," + record.genRecordStr(","));
              isFirst = false;
            } else {
              writer.write("\n" + fileId + "," + record.genRecordStr(","));
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
