package cn.edu.thu;

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SmallTest {

  static Schema schemas = new Schema();
  static File file = new File("tmp.tsfile");
  static TsFileWriter writer;

  static long rowNum = 10000L;
  static int value = 0;
  static int timestamp = 0;
  static int sensorNum = 20;

//  public static long insertBatch(int i) throws IOException, WriteProcessException {
//    long start = System.nanoTime();
//    RowBatch rowBatch = schemas.createRowBatch("device_" + i);
//    long[] timestamps = rowBatch.timestamps;
//    Object[] sensors = rowBatch.values;
//    for(int r = 0; r < rowNum; r++, value++){
//      int row = rowBatch.batchSize++;
//      timestamps[row] = timestamp++;
//      for(int j = 0; j < sensorNum; j++){
//        long[] sensor = (long[]) sensors[j];
//        sensor[row] = value;
//      }
//      if(rowBatch.batchSize == rowBatch.getMaxBatchSize()){
//        writer.write(rowBatch);
//        rowBatch.reset();
//      }
//    }
//    if(rowBatch.batchSize != 0){
//      writer.write(rowBatch);
//      rowBatch.reset();
//    }
//    return System.nanoTime() - start;
//  }

  public static long insertBatch(int i) throws IOException, WriteProcessException {
    long t = 0, s = 0;
    RowBatch rowBatch = schemas.createRowBatch("device_" + i);
    long[] timestamps = rowBatch.timestamps;
    Object[] sensors = rowBatch.values;
    for(int r = 0; r < rowNum; r++, value++){
      int row = rowBatch.batchSize++;
      timestamps[row] = timestamp++;
      for(int j = 0; j < sensorNum; j++){
        long[] sensor = (long[]) sensors[j];
        sensor[row] = value;
      }
      if(rowBatch.batchSize == rowBatch.getMaxBatchSize()){
        s  =System.nanoTime();
        writer.write(rowBatch);
        t += (System.nanoTime() - s);
        rowBatch.reset();
      }
    }
    if(rowBatch.batchSize != 0){
      s = System.nanoTime();
      writer.write(rowBatch);
      t += (System.nanoTime() - s);
      rowBatch.reset();
    }
    return t;
  }

  public static void main(String args[]) throws IOException, WriteProcessException {
    if (args.length > 0) {
      rowNum = Integer.parseInt(args[0]);
      sensorNum = Integer.parseInt(args[1]);
    }
    if (file.exists()) file.delete();
    writer = new TsFileWriter(file);
    for (int i = 0; i < sensorNum; i++) {
      MeasurementSchema schema = new MeasurementSchema("sensor" + i, TSDataType.INT64,
              TSEncoding.RLE, CompressionType.SNAPPY);
      schemas.registerMeasurement(schema);
      writer.addMeasurement(schema);
    }
    long time = 0;
    long pnt = 0;
    for(int i = 0; i < 10; i++){
      time += insertBatch(i);
      pnt += rowNum * sensorNum;
    }
    long s = System.nanoTime();
    writer.close();
    time += (System.nanoTime() - s);
    System.out.println(((double)pnt)/time * 1000_000_000);
  }
}
