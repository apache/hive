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
import org.apache.hadoop.hive.ql.session.SessionState;
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
  private RenderStrategy.InPlaceUpdateFunction updateFunction;

  @Before
  public void setUp() {
    curOut = System.out;
    curErr = System.err;
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
    testConf = new HiveConf();
    SessionState.start(testConf);
    monitor = new SparkJobMonitor(testConf) {
      @Override
      public int startMonitor() {
        return 0;
      }
    };
    updateFunction = new RenderStrategy.InPlaceUpdateFunction(monitor);
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
  public void testProgress() {
    Map<SparkStage, SparkStageProgress> progressMap = progressMap();
    updateFunction.printStatus(progressMap, null);
    String testOutput = errContent.toString();
    assertTrue(testOutput.contains(
        "Stage-1_0: 3(+1)/4\tStage-3_1: 4(+1,-1)/6\tStage-9_0: 5/5 Finished\tStage-10_2: 3(+2)/5\t"
            + "Stage-15_1: 3(+1)/4\tStage-15_2: 4/4 Finished\tStage-20_3: 1(+1,-1)/3\tStage-21_1: 2/2 Finished"));
    String[] testStrings = new String[]{
        "STAGES   ATTEMPT        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED",
        "Stage-1 ......           0       RUNNING      4          3        1        0       0",
        "Stage-3 .....            1       RUNNING      6          4        1        1       1",
        "Stage-9 ........         0      FINISHED      5          5        0        0       0",
        "Stage-10 ....            2       RUNNING      5          3        2        0       0",
        "Stage-15 .....           1       RUNNING      4          3        1        0       0",
        "Stage-15 .......         2      FINISHED      4          4        0        0       0",
        "Stage-20 ..              3       RUNNING      3          1        1        1       1",
        "Stage-21 .......         1      FINISHED      2          2        0        0       0",
        "STAGES: 03/08    [===================>>-------] 75%   ELAPSED TIME:"};
    for(String testString : testStrings) {
      assertTrue(testOutput.contains(testString));
    }
  }

  @After
  public void tearDown() {
    System.setOut(curOut);
    System.setErr(curErr);
  }
}
