package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.filter2.predicate.FilterApi.*;


/**
 * time, seriesid, value
 * <p>
 * time, deviceId, s1, s2, s3...
 * <p>
 * time, series1, series2...
 */
public class ParquetManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(ParquetManager.class);
  private MessageType schema;
  private ParquetWriter writer;
  private SimpleGroupFactory simpleGroupFactory;
  private Config config;
  private String filePath;
  private String schemaName = "defaultSchema";

  private int extendedColumnNumber;
  private List<List<Boolean>> extendedColumnNullValues;

  public ParquetManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
    extendedColumnNumber = config.NOAA_EXTENDED_COLUMN_NUMBER;
    extendedColumnNullValues = Config.NOAA_NULL_VALUES_MAP.get(extendedColumnNumber);
  }

  public ParquetManager(Config config, int threadNum) {
    this.config = config;
    this.filePath = config.FILE_PATH + "_" + threadNum;
    extendedColumnNumber = config.NOAA_EXTENDED_COLUMN_NUMBER;
    extendedColumnNullValues = Config.NOAA_NULL_VALUES_MAP.get(extendedColumnNumber);
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {
    if (Config.FOR_QUERY) {
      return;
    }

    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64,
            config.TIME_NAME));
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
            Config.TAG_NAME));
    for (int i = 0; i < config.FIELDS.length; i++) {
      builder.addField(
          new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE,
              config.FIELDS[i]));
    }

    schema = builder.named(schemaName);
    simpleGroupFactory = new SimpleGroupFactory(schema);
    Configuration configuration = new Configuration();

    GroupWriteSupport.setSchema(schema, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    groupWriteSupport.init(configuration);
    new File(filePath).delete();
    try {
      writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
          ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
          ParquetWriter.DEFAULT_PAGE_SIZE, true, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public long insertBatch(List<Record> records) {
    long start = System.nanoTime();

    List<Group> groups = convertRecords(records);
    for (Group group : groups) {
      try {
        writer.write(group);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return System.nanoTime() - start;
  }


  private List<Group> convertRecords(List<Record> records) {
    List<Group> groups = new ArrayList<>();
    for (Record record : records) {
      for (int i = 0; i < extendedColumnNumber; ++i) {
        List<Boolean> nullValues = extendedColumnNullValues.get(i);
        Group group = simpleGroupFactory.newGroup();
        group.add(Config.TIME_NAME, record.timestamp);
        if (!nullValues.get(nullValues.size() - 1)) {
          group.add(Config.TAG_NAME, record.tag);
        }
        for (int j = 0; j < config.FIELDS.length; j++) {
          double floatV = (double) record.fields.get(j);
          if (!nullValues.get(j)) {
            group.add(config.FIELDS[j], floatV);
          }
        }
        groups.add(group);
      }
    }
    return groups;
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    Configuration conf = new Configuration();
    ParquetInputFormat
        .setFilterPredicate(conf, and(and(gtEq(longColumn(Config.TIME_NAME), startTime),
            ltEq(longColumn(Config.TIME_NAME), endTime)),
            eq(binaryColumn(Config.TAG_NAME), Binary.fromString(tagValue))));
    FilterCompat.Filter filter = ParquetInputFormat.getFilter(conf);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64,
            Config.TIME_NAME));
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
            Config.TAG_NAME));
    builder.addField(
        new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, field));

    MessageType querySchema = builder.named(schemaName);
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

    // set reader
    ParquetReader.Builder<Group> reader = ParquetReader
        .builder(new GroupReadSupport(), new Path(filePath))
        .withConf(conf)
        .withFilter(filter);

    long start = System.nanoTime();

    ParquetReader<Group> build;
    int result = 0;
    try {
      build = reader.build();
      Group line;
      while ((line = build.read()) != null) {
        result++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.info("Parquet result: {}", result);

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
