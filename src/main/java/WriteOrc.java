import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class WriteOrc {

  public static void main(String [ ] args) throws java.io.IOException
  {
    File f = new File("my-file.orc");
    if(f.exists()) f.delete();
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.createStruct()
            .addField("int_value", TypeDescription.createInt())
            .addField("long_value", TypeDescription.createLong())
            .addField("double_value", TypeDescription.createDouble())
            .addField("float_value", TypeDescription.createFloat())
            .addField("boolean_value", TypeDescription.createBoolean())
            .addField("string_value", TypeDescription.createString());

    Writer writer = OrcFile.createWriter(new Path("my-file.orc"),
            OrcFile.writerOptions(conf)
                    .setSchema(schema));


    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector intVector = (LongColumnVector) batch.cols[0];
    LongColumnVector longVector = (LongColumnVector) batch.cols[1];
    DoubleColumnVector doubleVector = (DoubleColumnVector) batch.cols[2];
    DoubleColumnVector floatColumnVector = (DoubleColumnVector) batch.cols[3];
    LongColumnVector booleanVector = (LongColumnVector) batch.cols[4];
    BytesColumnVector stringVector = (BytesColumnVector) batch.cols[5];


    for(int r=0; r < 10; ++r) {
      int row = batch.size++;

      intVector.vector[row] = r;
      longVector.vector[row] = r;
      doubleVector.vector[row] = r;
      floatColumnVector.vector[row] = r;
      booleanVector.vector[row] =  r< 50000 ? 1 : 0;
//      stringVector.setVal(row, UUID.randomUUID().toString().getBytes());
      stringVector.setVal(row, String.valueOf("hello").getBytes(StandardCharsets.UTF_8));
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
  }


}

