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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.sql.Timestamp;

import static junit.framework.Assert.assertEquals;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestColumnStatistics {

  @Test
  public void testLongMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaIntObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateInteger(10);
    stats1.updateInteger(10);
    stats2.updateInteger(1);
    stats2.updateInteger(1000);
    stats1.merge(stats2);
    IntegerColumnStatistics typed = (IntegerColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum());
    assertEquals(1000, typed.getMaximum());
    stats1.reset();
    stats1.updateInteger(-10);
    stats1.updateInteger(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum());
    assertEquals(10000, typed.getMaximum());
  }

  @Test
  public void testDoubleMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateDouble(10.0);
    stats1.updateDouble(100.0);
    stats2.updateDouble(1.0);
    stats2.updateDouble(1000.0);
    stats1.merge(stats2);
    DoubleColumnStatistics typed = (DoubleColumnStatistics) stats1;
    assertEquals(1.0, typed.getMinimum(), 0.001);
    assertEquals(1000.0, typed.getMaximum(), 0.001);
    stats1.reset();
    stats1.updateDouble(-10);
    stats1.updateDouble(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum(), 0.001);
    assertEquals(10000, typed.getMaximum(), 0.001);
  }


  @Test
  public void testStringMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));
    stats2.updateString(new Text("anne"));
    stats2.updateString(new Text("erin"));
    stats1.merge(stats2);
    StringColumnStatistics typed = (StringColumnStatistics) stats1;
    assertEquals("anne", typed.getMinimum());
    assertEquals("erin", typed.getMaximum());
    stats1.reset();
    stats1.updateString(new Text("aaa"));
    stats1.updateString(new Text("zzz"));
    stats1.merge(stats2);
    assertEquals("aaa", typed.getMinimum());
    assertEquals("zzz", typed.getMaximum());
  }

  @Test
  public void testDateMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaDateObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateDate(new DateWritable(1000));
    stats1.updateDate(new DateWritable(100));
    stats2.updateDate(new DateWritable(10));
    stats2.updateDate(new DateWritable(2000));
    stats1.merge(stats2);
    DateColumnStatistics typed = (DateColumnStatistics) stats1;
    assertEquals(new DateWritable(10), typed.getMinimum());
    assertEquals(new DateWritable(2000), typed.getMaximum());
    stats1.reset();
    stats1.updateDate(new DateWritable(-10));
    stats1.updateDate(new DateWritable(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().getDays());
    assertEquals(10000, typed.getMaximum().getDays());
  }

  @Test
  public void testTimestampMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateTimestamp(new Timestamp(10));
    stats1.updateTimestamp(new Timestamp(100));
    stats2.updateTimestamp(new Timestamp(1));
    stats2.updateTimestamp(new Timestamp(1000));
    stats1.merge(stats2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().getTime());
    assertEquals(1000, typed.getMaximum().getTime());
    stats1.reset();
    stats1.updateTimestamp(new Timestamp(-10));
    stats1.updateTimestamp(new Timestamp(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().getTime());
    assertEquals(10000, typed.getMaximum().getTime());
  }

  @Test
  public void testDecimalMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateDecimal(HiveDecimal.create(10));
    stats1.updateDecimal(HiveDecimal.create(100));
    stats2.updateDecimal(HiveDecimal.create(1));
    stats2.updateDecimal(HiveDecimal.create(1000));
    stats1.merge(stats2);
    DecimalColumnStatistics typed = (DecimalColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().longValue());
    assertEquals(1000, typed.getMaximum().longValue());
    stats1.reset();
    stats1.updateDecimal(HiveDecimal.create(-10));
    stats1.updateDecimal(HiveDecimal.create(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().longValue());
    assertEquals(10000, typed.getMaximum().longValue());
  }
}
