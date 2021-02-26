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

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ORCTester implements Tester {

  private static Logger logger = LoggerFactory.getLogger(ORCTester.class);

  private int sensorNumber;

  private String filePath = "meta.orc";
  private Writer writer;
  private TypeDescription schema;

  public ORCTester(int sensorNumber) {
    this.sensorNumber = sensorNumber;

    schema = TypeDescription.fromString(genWriteSchema());
    new File(filePath).delete();
    try {
      writer = OrcFile.createWriter(new Path(filePath),
          OrcFile.writerOptions(new Configuration())
              .setSchema(schema)
              .compress(CompressionKind.SNAPPY)
              .version(OrcFile.Version.V_0_12));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String genWriteSchema() {
    String s = "struct<timestamp:bigint,deviceId:string";
    for (int i = 0; i < sensorNumber; i++) {
      s += ("," + "s" + i + ":" + "DOUBLE");
    }
    s += ">";
    return s;
  }

  @Override
  public void insertBatch(List<Record> records) {
    VectorizedRowBatch batch = schema.createRowBatch(records.size());

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];
      time.vector[i] = record.timestamp;

      BytesColumnVector device = (BytesColumnVector) batch.cols[1];
      device.setVal(i, record.tag.getBytes(StandardCharsets.UTF_8));

      for (int j = 0; j < sensorNumber; j++) {
        DoubleColumnVector v = (DoubleColumnVector) batch.cols[j + 2];
        v.vector[i] = (double) record.fields.get(j);
      }

      batch.size++;

      // If the batch is full, write it out and start over. actually not needed here
      if (batch.size == batch.getMaxSize()) {
        try {
          writer.addRowBatch(batch);
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch.reset();
      }
    }
  }

  @Override
  public void flush() {
    try {
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String getReadSchema(String field) {
    return "struct<timestamp:bigint,deviceId:string," + field + ":DOUBLE>";
  }

  @Override
  public float query() {
    long start = System.nanoTime();

    long startTime = 0;
    long endTime = 1946816515000L;

    String schema = getReadSchema("s1");
    try {
      Reader reader = OrcFile.createReader(new Path(filePath),
          OrcFile.readerOptions(new Configuration()));
      TypeDescription readSchema = TypeDescription.fromString(schema);

      VectorizedRowBatch batch = readSchema.createRowBatch();
      RecordReader rowIterator = reader.rows(reader.options().schema(readSchema));

      int fieldId;

      for (fieldId = 0; fieldId < sensorNumber; fieldId++) {
        if ("s1".endsWith("s" + fieldId)) {
          break;
        }
      }

      int result = 0;
      while (rowIterator.nextBatch(batch)) {
        for (int r = 0; r < batch.size; ++r) {

          // time, deviceId, field
          long t = ((LongColumnVector) batch.cols[0]).vector[r];
          if (t < startTime || t > endTime) {
            continue;
          }

          String deviceId = ((BytesColumnVector) batch.cols[1]).toString(r);

          if (deviceId.endsWith("device")) {
            result++;
          }

          double fieldValue = ((DoubleColumnVector) batch.cols[2]).vector[r];
        }
      }
      rowIterator.close();

      logger.info("ORC result: {}", result);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return (float) (System.nanoTime() - start) / 1000_000F;
  }
}
