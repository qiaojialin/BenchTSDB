import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.IOException;

public class TestWrite {
  public static void main(String args[]) throws IOException {
    String path = "test.orc";
    new File(path).delete();
    Configuration conf = new Configuration();
//    TypeDescription schema = TypeDescription.fromString("struct<timestamp:bigint,deviceId:string,TMP:double>");
    TypeDescription schema = TypeDescription.createStruct()
            .addField("timestamp", TypeDescription.createInt())
            .addField("deviceId", TypeDescription.createString())
            .addField("TMP", TypeDescription.createDouble());

    Writer writer = OrcFile.createWriter(new Path(path),
            OrcFile.writerOptions(conf)
                    .setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector timestamp = (LongColumnVector) batch.cols[0];
    BytesColumnVector devices = (BytesColumnVector)batch.cols[1];
    DoubleColumnVector tmp = (DoubleColumnVector) batch.cols[2];
    for(int r=0; r < 10; ++r) {
      int row = batch.size++;
      timestamp.vector[row] = r;
      devices.setVal(row, ("hello").getBytes());
      System.out.println(new String(devices.vector[row]));
      tmp.vector[row] = r * 3;
      // If the batch is full, write it out and start over.
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
