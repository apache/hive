/**
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

package org.apache.orc.impl;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestColumnStatisticsImpl {

  @Test
  public void testUpdateDate() throws Exception {
    ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(TypeDescription.createDate());
    DateWritable date = new DateWritable(16400);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 1, 16400, 16400);

    date.set(16410);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 2, 16400, 16410);

    date.set(16420);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 3, 16400, 16420);
  }

  private void assertDateStatistics(ColumnStatisticsImpl stat, int count, int minimum, int maximum) {
    OrcProto.ColumnStatistics.Builder builder = stat.serialize();

    assertEquals(count, builder.getNumberOfValues());
    assertTrue(builder.hasDateStatistics());
    assertFalse(builder.hasStringStatistics());

    OrcProto.DateStatistics protoStat = builder.getDateStatistics();
    assertTrue(protoStat.hasMinimum());
    assertEquals(minimum, protoStat.getMinimum());
    assertTrue(protoStat.hasMaximum());
    assertEquals(maximum, protoStat.getMaximum());
  }
}
