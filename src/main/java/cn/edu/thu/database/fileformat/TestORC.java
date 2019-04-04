package cn.edu.thu.database.fileformat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

public class TestORC {

  public static void main(String... args) throws IOException {

    Path testFilePath = new Path("advanced-example.orc");
    Configuration conf = new Configuration();

    TypeDescription schema =
        TypeDescription.fromString("struct<first:int," +
            "second:string>");

    Writer writer =
        OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf).setSchema(schema));

    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector first = (LongColumnVector) batch.cols[0];
    BytesColumnVector second = (BytesColumnVector) batch.cols[1];

//Define map. You need also to cast the key and value vectors
//    MapColumnVector map = (MapColumnVector) batch.cols[2];
//    BytesColumnVector mapKey = (BytesColumnVector) map.keys;
//    LongColumnVector mapValue = (LongColumnVector) map.values;

// Each map has 5 elements
    final int MAP_SIZE = 5;
    final int BATCH_SIZE = batch.getMaxSize();

// Ensure the map is big enough
//    mapKey.ensureSize(BATCH_SIZE * MAP_SIZE, false);
//    mapValue.ensureSize(BATCH_SIZE * MAP_SIZE, false);

// add 1500 rows to file
    for(int r=0; r < 1500; ++r) {
      int row = batch.size++;

      first.vector[row] = r;
//      second.vector[row] = r * 3;

      second.setVal(row, "aaa".getBytes());

//      map.offsets[row] = map.childCount;
//      map.lengths[row] = MAP_SIZE;
//      map.childCount += MAP_SIZE;
//
//      for (int mapElem = (int) map.offsets[row];
//          mapElem < map.offsets[row] + MAP_SIZE; ++mapElem) {
//        String key = "row " + r + "." + (mapElem - map.offsets[row]);
//        mapKey.setVal(mapElem, key.getBytes(StandardCharsets.UTF_8));
//        mapValue.vector[mapElem] = mapElem;
//      }
      if (row == BATCH_SIZE - 1) {
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
