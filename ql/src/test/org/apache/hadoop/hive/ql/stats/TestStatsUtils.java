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

package org.apache.hadoop.hive.ql.stats;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestStatsUtils {

  @Test
  public void testCombinedRange1() {
    Range r1 = new Range(0, 1);
    Range r2 = new Range(1, 11);
    Range r3 = StatsUtils.combineRange(r1, r2);
    assertNotNull(r3);
    rangeContains(r3, 0);
    rangeContains(r3, 1);
    rangeContains(r3, 11);
  }

  @Test
  public void testCombinedRange2() {
    checkCombinedRange(false, new Range(-2, -1), new Range(0, 10));
    checkCombinedRange(true, new Range(-2, 1), new Range(0, 10));
    checkCombinedRange(true, new Range(-2, 11), new Range(0, 10));
    checkCombinedRange(true, new Range(1, 2), new Range(0, 10));
    checkCombinedRange(true, new Range(1, 11), new Range(0, 10));
    checkCombinedRange(false, new Range(11, 12), new Range(0, 10));
  }

  private void checkCombinedRange(boolean valid, Range r1, Range r2) {
    Range r3a = StatsUtils.combineRange(r1, r2);
    Range r3b = StatsUtils.combineRange(r2, r1);
    if (valid) {
      assertNotNull(r3a);
      assertNotNull(r3b);
    } else {
      assertNull(r3a);
      assertNull(r3b);
    }
  }

  private boolean rangeContains(Range range, Number f) {
    double m = range.minValue.doubleValue();
    double M = range.maxValue.doubleValue();
    double v = f.doubleValue();
    return m <= v && v <= M;
  }

  @Test
  public void testPrimitiveSizeEstimations() throws Exception {
    HiveConf conf = new HiveConf();
    Set<String> exclusions = Sets.newHashSet();
    exclusions.add(serdeConstants.VOID_TYPE_NAME);
    exclusions.add(serdeConstants.LIST_TYPE_NAME);
    exclusions.add(serdeConstants.MAP_TYPE_NAME);
    exclusions.add(serdeConstants.STRUCT_TYPE_NAME);
    exclusions.add(serdeConstants.UNION_TYPE_NAME);
    Field[] serdeFields = serdeConstants.class.getFields();
    for (Field field : serdeFields) {
      if (!Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      if (!field.getName().endsWith("_TYPE_NAME")) {
        continue;
      }
      String typeName = (String) FieldUtils.readStaticField(field);
      if (exclusions.contains(typeName)) {
        continue;
      }
      int maxVarLen = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAX_VARIABLE_LENGTH);
      long siz = StatsUtils.getSizeOfPrimitiveTypeArraysFromType(typeName, 3, maxVarLen);
      assertNotEquals(field.toString(), 0, siz);
    }
  }

}