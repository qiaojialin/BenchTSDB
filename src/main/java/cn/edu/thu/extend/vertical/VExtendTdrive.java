package cn.edu.thu.extend.vertical;

import cn.edu.thu.extend.ExtendTdrive;
import cn.edu.thu.extend.record.TDriveRecord;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class VExtendTdrive extends ExtendTdrive {

  public VExtendTdrive(List<String> files, int copyNum, String outputDir, int offset) {
    super(files, copyNum, outputDir, offset);
  }

  @Override
  protected void write(int fileNum, Set<TDriveRecord> recordSet) throws IOException {
    int fileId = (fileNum + offset);
    FileWriter writer = new FileWriter(outputDir + fileId + ".txt");
    boolean isFirst = true;
    long first = 0, last = 0;
    for (TDriveRecord record : recordSet) {
      if (isFirst) {
        first = record.getTime();
        last = record.getTime();
        writer.write(fileId + "," + record.genRecordStr(","));
        isFirst = false;
      } else {
        last = record.getTime();
        writer.write("\n" + fileId + "," + record.genRecordStr(","));
      }
    }
    long diff = last - first + 1;
    for (int t = 1; t < copyNum; t++) {
      for (TDriveRecord record : recordSet) {
        record.setTime(record.getTime() + diff);
        writer.write("\n" + fileId + "," + record.genRecordStr(","));
      }
    }
    writer.close();
  }
}
