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
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(TsFileManager.class);
  private TsFileWriter writer;
  private String filePath;
  private Config config;

  public TsFileManager(Config config, String filePath) {
    this.config = config;
    this.filePath = filePath;
  }


  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {
    if (Config.FOR_QUERY) {
      return;
    }

    File file = new File(filePath);
    try {
      writer = new TsFileWriter(file);
      for (int i = 0; i < config.FIELDS.length; i++) {
        Map<String, String> props = new HashMap<>();
        props.put(Encoder.MAX_POINT_NUMBER, config.PRECISION[i] + "");
        MeasurementSchema schema = new MeasurementSchema(config.FIELDS[i], TSDataType.DOUBLE,
            TSEncoding.RLE, CompressionType.SNAPPY, props);
        writer.addMeasurement(schema);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public long insertBatch(List<Record> records) {
    long start = System.nanoTime();
    List<TSRecord> tsRecords = convertToRecords(records);
    for (TSRecord tsRecord : tsRecords) {
      try {
        writer.write(tsRecord);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return System.nanoTime() - start;
  }


  private List<TSRecord> convertToRecords(List<Record> records) {
    List<TSRecord> tsRecords = new ArrayList<>();
    for (Record record : records) {
      TSRecord tsRecord = new TSRecord(record.timestamp, record.tag);
      for (int i = 0; i < config.FIELDS.length; i++) {
        double floatField = (double) record.fields.get(i);
        tsRecord.addTuple(new DoubleDataPoint(config.FIELDS[i], floatField));
      }
      tsRecords.add(tsRecord);
    }
    return tsRecords;
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    long start = System.nanoTime();
    try {
      TsFileSequenceReader reader = new TsFileSequenceReader(config.FILE_PATH);

      ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(tagValue + "." + field));
      IExpression filter = new SingleSeriesExpression(new Path(tagValue + "." + field),
          new AndFilter(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime)));

      QueryExpression queryExpression = QueryExpression.create(paths, filter);

      QueryDataSet queryDataSet = readTsFile.query(queryExpression);

      int i = 0;
      while (queryDataSet.hasNext()) {
        i++;
        queryDataSet.next();
      }

      logger.info("TsFile count result: {}", i);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.nanoTime();
    try {
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return System.nanoTime() - start;
  }
}
