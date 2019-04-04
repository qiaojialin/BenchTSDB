package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Utils;
import cn.edu.thu.database.IDataBaseManager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * time, s1, s2, s3...
 *
 */
public class ORCManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(ORCManager.class);
  private Writer writer;
  private TypeDescription schema;
  private Config config;
  private String filePath;

  public ORCManager(Config config, String filePath) {
    this.config = config;
    this.filePath = filePath;
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {
    if (Config.FOR_QUERY) {
      return;
    }

    schema = TypeDescription.fromString(genWriteSchema());
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

  @Override
  public long insertBatch(List<Record> records) {

    long start = System.nanoTime();

    VectorizedRowBatch batch = schema.createRowBatch(records.size());

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];
      time.vector[i] = record.timestamp;

      for (int j = 0; j < config.FIELDS.length; j++) {
        DoubleColumnVector v = (DoubleColumnVector) batch.cols[j + 1];
        v.vector[i] = (double) record.fields.get(j);
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

    return System.nanoTime() - start;
  }


  private String genWriteSchema() {
    String s = "struct<timestamp:bigint";
    for (int i = 0; i < config.FIELDS.length; i++) {
      s += ("," + config.FIELDS[i] + ":" + "DOUBLE");
    }
    s += ">";
    return s;
  }

  private String getReadSchema(String field) {
    return "struct<timestamp:bigint," + field + ":DOUBLE>";
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    long start = System.nanoTime();

    String schema = getReadSchema(field);
    try {
      Configuration conf = new Configuration();
      Reader reader = OrcFile.createReader(new Path(filePath),
          OrcFile.readerOptions(conf));
      TypeDescription readSchema = TypeDescription.fromString(schema);

      Reader.Options readerOptions = new Reader.Options(conf)
          .searchArgument(
              SearchArgumentFactory
                  .newBuilder()
                  .between(Config.TIME_NAME, PredicateLeaf.Type.LONG, 1L, 2L)
                  .build(),
              new String[]{Config.TIME_NAME}
          );


      VectorizedRowBatch batch = readSchema.createRowBatch();

      RecordReader rowIterator = reader.rows(readerOptions.schema(readSchema));



      int fieldId;

      for (fieldId = 0; fieldId < config.FIELDS.length; fieldId++) {
        if (field.endsWith(config.FIELDS[fieldId])) {
          break;
        }
      }

      int traverse = 0;
      int result = 0;
      while (rowIterator.nextBatch(batch)) {
        for (int r = 0; r < batch.size; ++r) {

          // time, field
          long t = ((LongColumnVector) batch.cols[0]).vector[r];

          traverse ++;
          System.out.println(traverse + " " + t);

          if (t < startTime || t > endTime) {
            continue;
          }
          result++;



          double fieldValue = ((DoubleColumnVector) batch.cols[1]).vector[r];
        }
      }
      rowIterator.close();

      logger.info("ORC result: {}", result);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.nanoTime();
    try {
      writer.close();
      start = System.nanoTime() - start;

      String crcfilePath = Utils.replaceLast(filePath, "/", "/.") + ".crc";
      File file = new File(crcfilePath);
      if(file.exists()) {
        file.delete();
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
    return start;
  }
}
