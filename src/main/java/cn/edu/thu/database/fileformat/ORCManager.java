package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class ORCManager implements IDataBaseManager {

  private Writer writer;
  private TypeDescription schema;
  private Config config;
  private String filePath;

  public ORCManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
  }

  @Override
  public long insertBatch(List<Record> records) {

    long start = System.currentTimeMillis();

    VectorizedRowBatch batch = schema.createRowBatch(records.size());

    for(int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];
      time.vector[i] = record.timestamp;

      BytesColumnVector device = (BytesColumnVector) batch.cols[1];
      device.setVal(i, record.tag.getBytes(StandardCharsets.UTF_8));

      for (int j = 0; j < config.FIELDS.length; j++) {
        DoubleColumnVector v = (DoubleColumnVector) batch.cols[j + 2];
        v.vector[i] = (float) record.fields.get(j);
      }

      batch.size++;

      // If the batch is full, write it out and start over. actually not needed here
      if (batch.size == batch.getMaxSize()) {
        try {
          writer.addRowBatch(batch);
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch.reset();
      }
    }

    return System.currentTimeMillis() - start;
  }

  @Override
  public void createSchema() {
    schema = TypeDescription.fromString(genStringSchema());
    new File(filePath).delete();
    try {
      writer = OrcFile.createWriter(new Path(filePath),
          OrcFile.writerOptions(new Configuration())
              .setSchema(schema)
              .compress(CompressionKind.SNAPPY)
              .version(OrcFile.Version.V_0_12));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String genStringSchema() {
    String s = "struct<timestamp:bigint,deviceId:string";
    for (int i = 0; i < config.FIELDS.length; i++) {
      s += ("," + config.FIELDS[i] + ":" + "FLOAT");
    }
    s += ">";
    return s;
  }


  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {
    return 0;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.currentTimeMillis();
    try {
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return System.currentTimeMillis() - start;
  }
}
