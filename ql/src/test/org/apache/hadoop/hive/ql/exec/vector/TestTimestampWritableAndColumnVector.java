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

package org.apache.hadoop.hive.ql.exec.vector;

import org.junit.Test;

import java.sql.Timestamp;
import java.util.Random;

import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.ql.util.TimestampUtils;

import static org.junit.Assert.*;

/**
 * Test for ListColumnVector
 */
public class TestTimestampWritableAndColumnVector {

  private static int TEST_COUNT = 5000;

  @Test
  public void testDouble() throws Exception {

    Random r = new Random(1234);
    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    Timestamp[] randTimestamps = new Timestamp[VectorizedRowBatch.DEFAULT_SIZE];

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r).toSqlTimestamp();
      randTimestamps[i] = randTimestamp;
      timestampColVector.set(i, randTimestamp);
    }
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Timestamp retrievedTimestamp = timestampColVector.asScratchTimestamp(i);
      Timestamp randTimestamp = randTimestamps[i];
      if (!retrievedTimestamp.equals(randTimestamp)) {
        assertTrue(false);
      }
      double randDouble = TimestampUtils.getDouble(randTimestamp);
      double retrievedDouble = timestampColVector.getDouble(i);
      if (randDouble != retrievedDouble) {
        assertTrue(false);
      }
    }
  }
}
