package cn.edu.thu.database.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;

public class OrcRead {
  public static void main(String args[]) throws IOException {
    Configuration conf = new Configuration();
//    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
//    Writer writer = OrcFile.createWriter(new Path("my-file.orc"),
//            OrcFile.writerOptions(conf)
//                    .setSchema(schema));

    Reader reader = OrcFile.createReader(new Path("data/redd_low/house_1/96.txt.orc"),
            OrcFile.readerOptions(conf));


    Reader.Options readerOptions = new Reader.Options(conf)
            .searchArgument(
                    (SearchArgument) SearchArgumentFactory
                            .newBuilder()
                            .between("time", PredicateLeaf.Type.LONG, 0L,1024L)
                            .build(),
                    new String[]{"time"}
            );
    RecordReader rows = reader.rows(readerOptions);

//    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();

    int size = 0;
    while (rows.nextBatch(batch)) {
      System.out.println(batch.size);
      for (int r = 0; r < batch.size; ++r) {
        size++;
        long i = ((LongColumnVector) batch.cols[0]).vector[r];
        double j = ((DoubleColumnVector) batch.cols[1]).vector[r];
        System.out.println(size + " "+ i + " " + j);
      }
    }
    rows.close();

  }
}
