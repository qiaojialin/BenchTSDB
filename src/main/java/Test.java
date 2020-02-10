import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;

public class Test {
  public static void main(String args[]) throws IOException {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.createStruct()
            .addField("timestamp", TypeDescription.createInt())
            .addField("deviceId", TypeDescription.createString())
//            .addField("longitude",TypeDescription.createDouble())
            ;

//
//    Reader reader = OrcFile.createReader(new Path("test.orc"),
//            OrcFile.readerOptions(conf));
//    RecordReader rows = reader.rows();
//    VectorizedRowBatch batch = reader.getSchema().createRowBatch();

    Reader reader = OrcFile.createReader(new Path("tdrive.orc"),
            OrcFile.readerOptions(conf));

    Reader.Options readerOptions = new Reader.Options(conf);
    RecordReader rows = reader.rows(readerOptions.schema(schema));
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        long time = ((LongColumnVector)batch.cols[0]).vector[r];
        String deviceId = ((BytesColumnVector)batch.cols[1]).toString(r);
        System.out.println(time + " " + deviceId);
//        double tmp = ((DoubleColumnVector)batch.cols[2]).vector[r];
      }

    }
    rows.close();
  }

}
