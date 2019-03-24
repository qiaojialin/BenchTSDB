package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
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

    VectorizedRowBatch batch = schema.createRowBatch();

    return 0;
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
    String s = "struct<timestamp:bigint";
    for (int i = 0; i < config.FIELDS.length; i++) {
      s += ("," + config.FIELDS[i] + ":" + TSDataType.FLOAT.toString());
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
    return 0;
  }
}
