import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class ReadParquet {

  public static void main(String args[]) throws IOException {
    Configuration configuration = new Configuration();
    String COLUMN_PRIFIX = "c";
    int selectNum = 8;
    String schemaName = "Record";
    String filePath = "file.parquet";

    // set filter
    ParquetInputFormat.setFilterPredicate(configuration, eq(binaryColumn("name"), Binary.fromString("10_")));
    FilterCompat.Filter filter = ParquetInputFormat.getFilter(configuration);

    // set schema
    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "id"));
    for(int i = 0; i < selectNum; i++)
      builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, COLUMN_PRIFIX+ i));
    MessageType querySchema = builder.named(schemaName);
    configuration.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

    // set reader, withConf set specific fields (requested projection), withFilter set the filter.
    // if omit withConf, it queries all fields
    ParquetReader.Builder<Group> reader= ParquetReader
            .builder(new GroupReadSupport(), new Path(filePath))
            .withConf(configuration)
            .withFilter(filter);

    // read
    ParquetReader<Group> build=reader.build();
    Group line;
    while((line=build.read())!=null)
      System.out.println(line.toString());

  }
}