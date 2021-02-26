/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.thu.meta;

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
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import cn.edu.thu.common.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileTester implements Tester {

  private static Logger logger = LoggerFactory.getLogger(TsFileTester.class);

  private int sensorNumber;

  private File file;
  private TsFileWriter writer;
  private List<MeasurementSchema> schemas = new ArrayList<>();

  public TsFileTester(int sensorNumber) {
    this.sensorNumber = sensorNumber;
    (new File("meta.tsfile")).delete();
    file = new File("meta.tsfile");
    try {
      writer = new TsFileWriter(file);
      Map<String, MeasurementSchema> template = new HashMap<>();
      for (int i = 0; i < sensorNumber; i++) {
        Map<String, String> props = new HashMap<>();
        props.put(Encoder.MAX_POINT_NUMBER, 6 + "");
        MeasurementSchema schema = new MeasurementSchema("s" + i, TSDataType.DOUBLE,
            TSEncoding.RLE, CompressionType.SNAPPY, props);
        template.put("s" + i, schema);
        schemas.add(schema);
      }
      writer.registerDeviceTemplate("device", template);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void insertBatch(List<Record> batch) {
    Tablet tablet = convertToTablet(batch);
    try {
      writer.write(tablet);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Tablet convertToTablet(List<Record> records) {
    Tablet tablet = new Tablet("device", schemas, records.size());
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (Record record : records) {
      int row = tablet.rowSize++;
      timestamps[row] = record.timestamp;
      for (int i = 0; i < sensorNumber; i++) {
        double[] sensor = (double[]) values[i];
        sensor[row] = (double) record.fields.get(i);
      }
    }
    return tablet;
  }

  @Override
  public void flush() {
    try {
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public float query() {
    long start = System.nanoTime();

    try {
      TsFileSequenceReader reader = new TsFileSequenceReader("meta.tsfile");

      ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("device", "s1"));
      IExpression filter = new SingleSeriesExpression(new Path("device.s1"),
          new AndFilter(TimeFilter.gtEq(0), TimeFilter.ltEq(1946816515000L)));

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

    return (float) (System.nanoTime() - start) / 1000_000F;
  }
}
