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
import org.apache.orc.*;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * time, seriesid, value
 * <p>
 * time, deviceId, s1, s2, s3...
 * <p>
 * time, series1, series2...
 */
public class ORCManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(ORCManager.class);
  private Writer writer;
  private TypeDescription schema;
  private Config config;
  private String filePath;

  private int extendedColumnNumber;
  private List<List<Boolean>> extendedColumnNullValues;

  public ORCManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
    extendedColumnNumber = config.NOAA_EXTENDED_COLUMN_NUMBER;
    extendedColumnNullValues = Config.NOAA_NULL_VALUES_MAP.get(extendedColumnNumber);
  }

  public ORCManager(Config config, int threadNum) {
    this.config = config;
    this.filePath = config.FILE_PATH + "_" + threadNum;
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

    VectorizedRowBatch batch = schema.createRowBatch(records.size() * extendedColumnNumber);

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);

      for (int j = 0; j < extendedColumnNumber; j++) {
        List<Boolean> nullValues = extendedColumnNullValues.get(j);
        final int indexInBatch = i * extendedColumnNumber + j;

        LongColumnVector time = (LongColumnVector) batch.cols[0];
        time.vector[indexInBatch] = record.timestamp;

        BytesColumnVector device = (BytesColumnVector) batch.cols[1];
        device.setVal(indexInBatch, record.tag.getBytes(StandardCharsets.UTF_8));
        device.isNull[indexInBatch] = nullValues.get(nullValues.size() - 1);

        for (int k = 0; k < config.FIELDS.length; k++) {
          DoubleColumnVector v = (DoubleColumnVector) batch.cols[k + 2];
          v.vector[indexInBatch] = (double) record.fields.get(k);
          v.isNull[indexInBatch] = nullValues.get(k);
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
    }

    return System.nanoTime() - start;
  }


  private String genWriteSchema() {
    String s = "struct<timestamp:bigint,deviceId:string";
    for (int i = 0; i < config.FIELDS.length; i++) {
      s += ("," + config.FIELDS[i] + ":" + "DOUBLE");
    }
    s += ">";
    return s;
  }

  private String getReadSchema(String field) {
    return "struct<timestamp:bigint,deviceId:string," + field + ":DOUBLE>";
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    long start = System.nanoTime();

    String schema = getReadSchema(field);
    try {
      Reader reader = OrcFile.createReader(new Path(filePath),
          OrcFile.readerOptions(new Configuration()));
      TypeDescription readSchema = TypeDescription.fromString(schema);

      VectorizedRowBatch batch = readSchema.createRowBatch();
      RecordReader rowIterator = reader.rows(reader.options().schema(readSchema));

      int fieldId;

      for (fieldId = 0; fieldId < config.FIELDS.length; fieldId++) {
        if (field.endsWith(config.FIELDS[fieldId])) {
          break;
        }
      }

      int result = 0;
      while (rowIterator.nextBatch(batch)) {
        for (int r = 0; r < batch.size; ++r) {

          // time, deviceId, field
          long t = ((LongColumnVector) batch.cols[0]).vector[r];
          if (t < startTime || t > endTime) {
            continue;
          }

          String deviceId = ((BytesColumnVector) batch.cols[1]).toString(r);

          if (deviceId.endsWith(tagValue)) {
            result++;
          }

          double fieldValue = ((DoubleColumnVector) batch.cols[2]).vector[r];
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
    } catch (IOException e) {
      e.printStackTrace();
    }
    return System.nanoTime() - start;
  }
}
