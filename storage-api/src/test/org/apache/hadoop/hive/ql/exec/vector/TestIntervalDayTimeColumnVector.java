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

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestIntervalDayTimeColumnVector {

  @Test
  void clearValueZeroesSlotAndMarksNull() {
    IntervalDayTimeColumnVector cv = new IntervalDayTimeColumnVector(4);
    cv.set(3, new HiveIntervalDayTime(5, 0));

    cv.clearValue(3);

    assertTrue(cv.isNull[3]);
    assertFalse(cv.noNulls);
    // setNullValue convention: totalSeconds = 0, nanos = 1
    assertEquals(0L, cv.getTotalSeconds(3));
    assertEquals(1, cv.getNanos(3));
  }
}
