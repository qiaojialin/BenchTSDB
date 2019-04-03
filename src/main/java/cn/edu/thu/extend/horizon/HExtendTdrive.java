package cn.edu.thu.extend.horizon;

import cn.edu.thu.extend.ExtendTdrive;
import cn.edu.thu.extend.record.TDriveRecord;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HExtendTdrive extends ExtendTdrive {

  private Logger logger = LoggerFactory.getLogger(HExtendTdrive.class);

  public HExtendTdrive(List<String> files, int copyNum, String outputDir, int offset) {
    super(files, copyNum, outputDir, offset);
  }

  protected void write(int fileNum, Set<TDriveRecord> recordSet) throws IOException {
    for (int t = 0; t < copyNum; t++) {
      int fileId = (fileNum + offset) * copyNum + t;
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
}
