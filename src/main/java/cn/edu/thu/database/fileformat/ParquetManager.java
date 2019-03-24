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
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class ParquetManager implements IDataBaseManager {

  private MessageType schema;
  private ParquetWriter writer;
  private SimpleGroupFactory simpleGroupFactory;
  private Config config;
  private String filePath;

  public ParquetManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
  }

  @Override
  public void createSchema() {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "timestamp"));
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, config.TAG_NAME));
    for (int i = 0; i < config.FIELDS.length; i++) {
      builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, config.FIELDS[i]));
    }

    schema = builder.named("defaultSchema");
    simpleGroupFactory = new SimpleGroupFactory(schema);
    Configuration configuration = new Configuration();

    GroupWriteSupport.setSchema(schema, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    groupWriteSupport.init(configuration);
    new File(filePath).delete();
    try {
      writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
          ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
          true, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public long insertBatch(List<Record> records) {
    long start = System.currentTimeMillis();

    List<Group> groups = convertRecords(records);
    for(Group group: groups) {
      try {
        writer.write(group);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return System.currentTimeMillis() - start;
  }


  private List<Group> convertRecords(List<Record> records) {
    List<Group> groups = new ArrayList<>();
    for(Record record: records) {
      Group group = simpleGroupFactory.newGroup();
      group.add("timestamp", record.timestamp);
      group.add(config.TAG_NAME, record.tag);
      for(int i = 0; i < config.FIELDS.length; i++) {
        float floatV = (float) record.fields.get(i);
        group.add(config.FIELDS[i], floatV);
      }
      groups.add(group);
    }
    return groups;
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
