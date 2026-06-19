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

class TestDecimalColumnVector {

  @Test
  void clearValueZeroesSlotAndMarksNull() {
    DecimalColumnVector cv = new DecimalColumnVector(4, 18, 4);
    cv.vector[1].setFromLong(12345L);
    cv.vector[2].setFromLong(67890L);

    cv.clearValue(1);

    assertTrue(cv.isNull[1]);
    assertFalse(cv.noNulls);
    assertEquals(0L, cv.vector[1].serialize64(cv.scale));
    // Neighbour slot untouched: still represents 67890.
    assertEquals(67890L, cv.vector[2].serialize64((short) 0));
    assertFalse(cv.isNull[2]);
  }
}
