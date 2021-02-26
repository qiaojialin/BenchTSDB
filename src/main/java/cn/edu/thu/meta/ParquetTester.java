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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;

public class ParquetTester implements Tester {

  private static Logger logger = LoggerFactory.getLogger(ParquetTester.class);

  private int sensorNumber;

  private MessageType schema;
  private ParquetWriter writer;
  private SimpleGroupFactory simpleGroupFactory;
  private String filePath = "meta.parquet";
  private String schemaName = "defaultSchema";

  public ParquetTester(int sensorNumber) {
    this.sensorNumber = sensorNumber;

    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64,
            Config.TIME_NAME));
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
            Config.TAG_NAME));
    for (int i = 0; i < sensorNumber; i++) {
      builder.addField(
          new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE,
              "s" + i));
    }

    schema = builder.named(schemaName);
    simpleGroupFactory = new SimpleGroupFactory(schema);
    Configuration configuration = new Configuration();

    GroupWriteSupport.setSchema(schema, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    groupWriteSupport.init(configuration);
    new File(filePath).delete();
    try {
      writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
          ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
          ParquetWriter.DEFAULT_PAGE_SIZE,
          true, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void insertBatch(List<Record> records) {
    List<Group> groups = convertRecords(records);
    for (Group group : groups) {
      try {
        writer.write(group);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private List<Group> convertRecords(List<Record> records) {
    List<Group> groups = new ArrayList<>();
    for (Record record : records) {
      Group group = simpleGroupFactory.newGroup();
      group.add(Config.TIME_NAME, record.timestamp);
      group.add(Config.TAG_NAME, record.tag);
      for (int i = 0; i < sensorNumber; i++) {
        double floatV = (double) record.fields.get(i);
        group.add("s" + i, floatV);
      }
      groups.add(group);
    }
    return groups;
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

    long startTime = 0;
    long endTime = 1946816515000L;

    Configuration conf = new Configuration();
    ParquetInputFormat
        .setFilterPredicate(conf, and(and(gtEq(longColumn(Config.TIME_NAME), startTime),
            ltEq(longColumn(Config.TIME_NAME), endTime)),
            eq(binaryColumn(Config.TAG_NAME), Binary.fromString("device"))));
    FilterCompat.Filter filter = ParquetInputFormat.getFilter(conf);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64,
            Config.TIME_NAME));
    builder.addField(
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,
            Config.TAG_NAME));
    builder.addField(
        new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "s1"));

    MessageType querySchema = builder.named(schemaName);
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

    // set reader
    ParquetReader.Builder<Group> reader = ParquetReader
        .builder(new GroupReadSupport(), new Path(filePath))
        .withConf(conf)
        .withFilter(filter);

    ParquetReader<Group> build;
    int result = 0;
    try {
      build = reader.build();
      Group line;
      while ((line = build.read()) != null) {
        result++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.info("Parquet result: {}", result);

    return (float) (System.nanoTime() - start) / 1000_000F;
  }
}
