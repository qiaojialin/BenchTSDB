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

import java.io.File;
import java.io.IOException;

public class WriteParquet {

  public static void main(String args[]) throws IOException {
    ParquetWriter writer;
    MessageType schema;

    int lineNumber = 10, columnNumber = 10;
    String COLUMN_PRIFIX = "c";
    PrimitiveType.PrimitiveTypeName typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
    String schemaName = "Record";
    Configuration configuration = new Configuration();
    String filePath = "file.parquet";
    boolean usingEncoing = true;

    // initialize writer
    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "id"));
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "name"));
    for (int i = 0; i < columnNumber; i++)
      builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,COLUMN_PRIFIX + i));
    schema = builder.named(schemaName);
    GroupWriteSupport.setSchema(schema, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    groupWriteSupport.init(configuration);
    new File(filePath).delete();
    writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
            ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
            usingEncoing, true, ParquetProperties.WriterVersion.PARQUET_2_0);

    SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);

    // write data
    for(int i = 0; i < lineNumber; i++){
      Group group = simpleGroupFactory.newGroup();
      group.add("id", (long)i);
      group.add("name", "" + i + "_");
      for(int j = 0; j < columnNumber; j++)
        group.add(COLUMN_PRIFIX + j, (float)Math.random());
      writer.write(group);
    }
    writer.close();

  }
}