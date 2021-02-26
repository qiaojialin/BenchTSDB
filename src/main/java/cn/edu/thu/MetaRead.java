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

package cn.edu.thu;

import cn.edu.thu.common.Record;
import cn.edu.thu.meta.DataGenerator;
import cn.edu.thu.meta.ORCTester;
import cn.edu.thu.meta.ParquetTester;
import cn.edu.thu.meta.Tester;
import cn.edu.thu.meta.TsFileTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MetaRead {

  private static Logger logger = LoggerFactory.getLogger(MetaRead.class);

  public static void main(String[] args) {

    doTest(1, 100);
    doTest(1, 1000);
    doTest(1, 10000);
    doTest(1, 100000);
  }

  private static void doTest(int rowNumber, int sensorNumber) {
    Tester[] testers = new Tester[] {
        new TsFileTester(sensorNumber),
        new ParquetTester(sensorNumber),
        new ORCTester(sensorNumber)
    };

    DataGenerator dataGenerator = new DataGenerator(rowNumber, sensorNumber);
    while (dataGenerator.hasNext()) {
      List<Record> records = dataGenerator.next();
      for (Tester tester : testers) {
        tester.insertBatch(records);
      }
    }

    for (Tester tester : testers) {
      tester.flush();
    }

    logger.info("<<<<<<<<<<<< N: {}", sensorNumber);
    logger.info("<<<<<<<<<<<< TsFile: {}", testers[0].query());
    logger.info("<<<<<<<<<<<< Parquet: {}", testers[1].query());
    logger.info("<<<<<<<<<<<< ORC: {}", testers[2].query());
    logger.info("<<<<<<<<<<<< row number: {}", rowNumber);
  }
}
