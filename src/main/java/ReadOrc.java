import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

public class ReadOrc {

  public static void main(String [ ] args) throws java.io.IOException
  {
    Configuration conf = new Configuration();

    TypeDescription readSchema = TypeDescription.createStruct()
//        .addField("int_value", TypeDescription.createInt())
            .addField("long_value", TypeDescription.createLong())
            .addField("double_value", TypeDescription.createDouble())
            .addField("float_value", TypeDescription.createFloat())
            .addField("boolean_value", TypeDescription.createBoolean())
            .addField("string_value", TypeDescription.createString());


    Reader reader = OrcFile.createReader(new Path("my-file.orc"),
            OrcFile.readerOptions(conf));

    Reader.Options readerOptions = new Reader.Options(conf)
            .searchArgument(
                    SearchArgumentFactory
                            .newBuilder()
                            .equals("string_value", PredicateLeaf.Type.STRING, "hello")
                            .between("long_value", PredicateLeaf.Type.LONG, 0L,20L)
                            .build(),
                    new String[]{"long_value"}
            );

    RecordReader rows = reader.rows(readerOptions.schema(readSchema));

    VectorizedRowBatch batch = readSchema.createRowBatch();

    while (rows.nextBatch(batch)) {
//      LongColumnVector intVector = (LongColumnVector) batch.cols[0];
      LongColumnVector longVector = (LongColumnVector) batch.cols[0];
      DoubleColumnVector doubleVector  = (DoubleColumnVector) batch.cols[1];
      DoubleColumnVector floatVector = (DoubleColumnVector) batch.cols[2];
      LongColumnVector booleanVector = (LongColumnVector) batch.cols[3];
      BytesColumnVector stringVector = (BytesColumnVector)  batch.cols[4];

      System.out.println(batch.size);

      for(int r=0; r < batch.size; r++) {
//        int intValue = (int) intVector.vector[r];
        long longValue = longVector.vector[r];
        double doubleValue = doubleVector.vector[r];
        double floatValue = (float) floatVector.vector[r];
        boolean boolValue = booleanVector.vector[r] != 0;
//        String stringValue = new String(stringVector.vector[r], stringVector.start[r], stringVector.length[r]);
        String stringValue = stringVector.toString(r);
        System.out.println( longValue + ", " + doubleValue + ", " + floatValue + ", " + boolValue + ", " + stringValue);

      }
    }
    rows.close();
  }

}

