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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestLongColumnVector {

  @Test
  void clearValueZeroesSlotAndMarksNull() {
    LongColumnVector cv = new LongColumnVector(4);
    cv.vector[2] = 2025L;
    cv.vector[1] = 7L;
    cv.vector[3] = 9L;

    cv.clearValue(2);

    assertTrue(cv.isNull[2]);
    assertFalse(cv.noNulls);
    assertEquals(0L, cv.vector[2]);
    assertEquals(7L, cv.vector[1]);
    assertEquals(9L, cv.vector[3]);
    assertFalse(cv.isNull[1]);
    assertFalse(cv.isNull[3]);
  }
}
