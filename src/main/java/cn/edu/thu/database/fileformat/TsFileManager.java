package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TsFileManager implements IDataBaseManager {

  private TsFileWriter writer;
  private String filePath = "a";
  private Config config;

  public TsFileManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
  }

  @Override
  public long insertBatch(List<Record> records) {
    long start = System.currentTimeMillis();
    List<TSRecord> tsRecords = convertToRecords(records);
    for(TSRecord tsRecord: tsRecords) {
      try {
        writer.write(tsRecord);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return System.currentTimeMillis() - start;
  }

  @Override
  public void createSchema() {
    File file = new File(filePath);
    try {
      writer = new TsFileWriter(file);
      for (int i = 0; i < config.FIELDS.length; i++) {
        Map<String, String> props = new HashMap<>();
        props.put(Encoder.MAX_POINT_NUMBER, config.PRECISION[i]+"");
        MeasurementSchema schema = new MeasurementSchema(config.FIELDS[i], TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY, props);
        writer.addMeasurement(schema);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private List<TSRecord> convertToRecords(List<Record> records) {
    List<TSRecord> tsRecords = new ArrayList<>();
    for(Record record: records) {
      TSRecord tsRecord = new TSRecord(record.timestamp, record.tag);
      for(int i = 0; i < config.FIELDS.length; i ++) {
        float floatField = (float) record.fields.get(i);
        tsRecord.addTuple(new FloatDataPoint(config.FIELDS[i], floatField));
      }
      tsRecords.add(tsRecord);
    }
    return tsRecords;
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
