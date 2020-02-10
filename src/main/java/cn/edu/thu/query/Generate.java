package cn.edu.thu.query;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Generate {

  private static Logger logger = LoggerFactory.getLogger(Generate.class);

  static long DEVICE_NUM = 100L;
  static long SENSOR_NUM = 10000L;
  static long ROW_NUM = 1000L;
//  static int BATCH_SIZE = 1024;

  static String currentDevice;

  static TsFileWriter tsFileWriter;
  static Schema tsFileSchema;

  static Writer orcWriter;
  static TypeDescription orcSchema;

  static ParquetWriter parquetWriter;
  static MessageType parquetSchema;
  static SimpleGroupFactory parquetSimpleGroupFactory;

  static long tsfileTime;
  static long orcTime;
  static long parquetTime;

  public static void initTsfile(String path) throws IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("REGULAR");
    File file = new File(path);
    if (file.exists())
      file.delete();
    tsFileWriter = new TsFileWriter(file);
    tsFileSchema = new Schema();
    for (int i = 0; i < SENSOR_NUM; i++) {
      MeasurementSchema measurementSchema = new MeasurementSchema("sensor_" + i, TSDataType.INT32,
              TSEncoding.RLE, CompressionType.SNAPPY);
      tsFileSchema.registerMeasurement(measurementSchema);
      tsFileWriter.addMeasurement(measurementSchema);
    }
  }

  public static void initOrc(String path) throws IOException {
    String s = "struct<timestamp:bigint,DEVICE:STRING";
    for (int i = 0; i < SENSOR_NUM; i++) {
      s += ("," + "sensor_" + i + ":" + "bigint");
    }
    s += ">";
    orcSchema = TypeDescription.fromString(s);
    File f = new File(path);
    if (f.exists())
      f.delete();
    orcWriter = OrcFile.createWriter(new Path(path),
            OrcFile.writerOptions(new Configuration())
                    .setSchema(orcSchema)
                    .compress(CompressionKind.SNAPPY)
                    .version(OrcFile.Version.V_0_12));
  }

  public static void initParquet(String path) throws IOException {
    File f = new File(path);
    if (f.exists())
      f.delete();
    String schemaName = "defaultSchema";
    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, Config.TIME_NAME));
    builder.addField(new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, Config.TAG_NAME));
    for (int i = 0; i < SENSOR_NUM; i++) {
      builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "sensor_" + i));
    }
    parquetSchema = builder.named(schemaName);
    parquetSimpleGroupFactory = new SimpleGroupFactory(parquetSchema);
    Configuration configuration = new Configuration();
    GroupWriteSupport.setSchema(parquetSchema, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    groupWriteSupport.init(configuration);
    new File(path).delete();
    parquetWriter = new ParquetWriter(new Path(path), groupWriteSupport, CompressionCodecName.SNAPPY,
            ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
            true, true, ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  static void init() throws IOException, WriteProcessException {
    initTsfile("data/out.tsfile");
    initOrc("data/out.orc");
    initParquet("data/out.parquet");
  }

  static void write(List<Record> rowBatch) {
    tsfileTime += writeTsfile(rowBatch);
    orcTime += writeOrc(rowBatch);
    parquetTime += writeParquet(rowBatch);
  }

  static void close() throws IOException {
    tsfileTime += closeTsfile();
    orcTime += closeOrc();
    parquetTime += closeParquet();
  }

  private static long closeParquet() throws IOException {
    long start = System.nanoTime();
    parquetWriter.close();
    return System.nanoTime() - start;
  }

  private static long closeOrc() throws IOException {
    long start = System.nanoTime();
    orcWriter.close();
    return System.nanoTime() - start;
  }

  private static long closeTsfile() throws IOException {
    long start = System.nanoTime();
    tsFileWriter.close();
    return System.nanoTime() - start;
  }

  private static long writeParquet(List<Record> rowBatch) {
    List<Group> groups = convertRecords(rowBatch);
    long start = System.nanoTime();
    for (Group group : groups) {
      try {
        parquetWriter.write(group);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return System.nanoTime() - start;
  }

  private static List<Group> convertRecords(List<Record> records) {
    List<Group> groups = new ArrayList<>();
    for (Record record : records) {
      Group group = parquetSimpleGroupFactory.newGroup();
      group.add(Config.TIME_NAME, record.timestamp);
      group.add(Config.TAG_NAME, record.tag);
      for (int i = 0; i < SENSOR_NUM; i++) {
        int floatV = (int) record.fields.get(i);
        group.add("sensor_" + i, floatV);
      }
      groups.add(group);
    }
    return groups;
  }

  private static long writeOrc(List<Record> rowBatch) {
    long s = System.nanoTime();
    VectorizedRowBatch batch = orcSchema.createRowBatch(rowBatch.size());

    for (int i = 0; i < rowBatch.size(); i++) {
      Record record = rowBatch.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];
      time.vector[i] = record.timestamp;
      BytesColumnVector device = (BytesColumnVector) batch.cols[1];
      device.vector[i] = record.tag.getBytes();
      for (int j = 0; j < SENSOR_NUM; j++) {
        LongColumnVector v = (LongColumnVector) batch.cols[j + 2];
        v.vector[i] = (int) record.fields.get(j);
      }
    }
    try {
      orcWriter.addRowBatch(batch);
      batch.reset();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return System.nanoTime() - s;
  }

  private static long writeTsfile(List<Record> rowBatch) {
    long start = System.nanoTime();
    RowBatch batch = tsFileSchema.createRowBatch(currentDevice, rowBatch.size());

    // i is the row index
    for (int i = 0; i < rowBatch.size(); i++) {
      Record record = rowBatch.get(i);
      long[] timestamps = batch.timestamps;
      timestamps[i] = record.timestamp;

      for (int j = 0; j < SENSOR_NUM; j++) {
        int[] value = (int[]) batch.values[j]; // TODO
        value[i] = (int) record.fields.get(j);
      }
      batch.batchSize++;

      if (batch.batchSize == batch.getMaxBatchSize()) {
        try {
          tsFileWriter.write(batch);
          tsFileWriter.flushForTest();
        } catch (IOException | WriteProcessException e) {
          e.printStackTrace();
        }
        batch.reset();
      }
    }

    if (batch.batchSize != 0) {
      try {
        tsFileWriter.write(batch);
        tsFileWriter.flushForTest();
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
      }
      batch.reset();
    }
    return System.nanoTime() - start;
  }

  /*device_num, sensor_num, row_num*/
  public static void main(String args[]) throws IOException, WriteProcessException {
    if (args.length != 0) {
      DEVICE_NUM = Long.parseLong(args[0]);
      SENSOR_NUM = Long.parseLong(args[1]);
      ROW_NUM = Long.parseLong(args[2]);
    }

    init();

    List<Record> rowBatch;

    for (int i = 0; i < DEVICE_NUM; i++) {
      logger.info("start to write device {}", i);
      rowBatch = new ArrayList<>();
      currentDevice = "device_" + i;
      for (int time = 1; time <= ROW_NUM; time++) {
        List<Object> row = new ArrayList<>();
        for (int j = 0; j < SENSOR_NUM; j++) {
          row.add(time);
        }
        rowBatch.add(new Record(time, currentDevice, row));
      }
      write(rowBatch);
    }
    close();
    long points = DEVICE_NUM * SENSOR_NUM * ROW_NUM;
    double tsfileSpeed = ((double) points) / tsfileTime;
    double parquetSpeed = ((double) points) / parquetTime;
    double orcSpeed = ((double) points) / orcTime;
    System.out.println("[TSFILE]: " + (tsfileSpeed * 1000_1000_1000L) + "points/s");
    System.out.println("[PARQUET]: " + (parquetSpeed * 1000_1000_1000L) + "points/s");
    System.out.println("[ORC]: " + (orcSpeed * 1000_1000_1000L) + "points/s");
  }
}
