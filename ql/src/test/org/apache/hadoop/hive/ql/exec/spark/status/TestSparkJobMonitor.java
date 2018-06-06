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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.status;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Test spark progress monitoring information.
 */
public class TestSparkJobMonitor {

  private HiveConf testConf;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private SparkJobMonitor monitor;
  private PrintStream curOut;
  private PrintStream curErr;

  @Before
  public void setUp() {
    testConf = new HiveConf();
    curOut = System.out;
    curErr = System.err;
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));

    monitor = new SparkJobMonitor(testConf) {
      @Override
      public int startMonitor() {
        return 0;
      }
    };

  }

  private Map<SparkStage, SparkStageProgress> progressMap() {
    return new HashMap<SparkStage, SparkStageProgress>() {{
        put(new SparkStage(1, 0), new SparkStageProgress(4, 3, 1, 0));
        put(new SparkStage(3, 1), new SparkStageProgress(6, 4, 1, 1));
        put(new SparkStage(9, 0), new SparkStageProgress(5, 5, 0, 0));
        put(new SparkStage(10, 2), new SparkStageProgress(5, 3, 2, 0));
        put(new SparkStage(15, 1), new SparkStageProgress(4, 3, 1, 0));
        put(new SparkStage(15, 2), new SparkStageProgress(4, 4, 0, 0));
        put(new SparkStage(20, 3), new SparkStageProgress(3, 1, 1, 1));
        put(new SparkStage(21, 1), new SparkStageProgress(2, 2, 0, 0));
      }};
  }

  @Test
  public void testGetReport() {
    Map<SparkStage, SparkStageProgress> progressMap = progressMap();
    monitor.printStatus(progressMap, null);
    assertTrue(errContent.toString().contains(
        "Stage-1_0: 3(+1)/4\tStage-3_1: 4(+1,-1)/6\tStage-9_0: 5/5 Finished\tStage-10_2: 3(+2)/5\t"
            + "Stage-15_1: 3(+1)/4\tStage-15_2: 4/4 Finished\tStage-20_3: 1(+1,-1)/3\tStage-21_1: 2/2 Finished"));
  }

  @After
  public void tearDown() {
    System.setOut(curOut);
    System.setErr(curErr);
  }
}
