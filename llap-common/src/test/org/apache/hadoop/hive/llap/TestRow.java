/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestRow {

  @Test
  public void testUsage() {
    Schema schema = createTestSchema();
    Row row = new Row(schema);

    Random rand = new Random();
    int iterations = 100;
    for (int idx = 0; idx < iterations; ++idx) {
      // Set the row values
      boolean isNullCol0 = (rand.nextDouble() <= 0.25);
      String col0 = RandomStringUtils.random(10);
      row.setValue(0, isNullCol0 ? null : col0);

      boolean isNullCol1 = (rand.nextDouble() <= 0.25);
      Integer col1 = Integer.valueOf(rand.nextInt());
      row.setValue(1, isNullCol1 ? null : col1);

      // Validate the row values
      if (isNullCol0) {
        assertTrue(row.getValue(0) == null);
        assertTrue(row.getValue("col0") == null);
      } else {
        assertTrue(row.getValue(0) != null);
        assertEquals(col0, row.getValue(0));
        assertEquals(col0, row.getValue("col0"));
      }

      if (isNullCol1) {
        assertTrue(row.getValue(1) == null);
        assertTrue(row.getValue("col1") == null);
      } else {
        assertTrue(row.getValue(1) != null);
        assertEquals(col1, row.getValue(1));
        assertEquals(col1, row.getValue("col1"));
      }
    }
  }

  private Schema createTestSchema() {
    List<FieldDesc> colDescs = new ArrayList<FieldDesc>();

    colDescs.add(new FieldDesc("col0",
        TypeInfoFactory.stringTypeInfo));

    colDescs.add(new FieldDesc("col1",
        TypeInfoFactory.intTypeInfo));

    Schema schema = new Schema(colDescs);
    return schema;
  }
}