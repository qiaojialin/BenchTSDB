package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
      writer = new TsFileWriter(new TsRandomAccessFileWriter(file));
      Map<String, String> props = new HashMap<>();
      props.put(JsonFormatConstant.COMPRESS_TYPE, CompressionTypeName.SNAPPY+"");
      for (int i = 0; i < config.FIELDS.length; i++) {
        MeasurementDescriptor descriptor = new MeasurementDescriptor(config.FIELDS[i], TSDataType.FLOAT, TSEncoding.RLE, props);
        writer.addMeasurement(descriptor);
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
    } catch (IOException e) {
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
