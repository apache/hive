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
package org.apache.hadoop.hive.ql.queryhistory.schema;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.junit.Assert;
import org.junit.Test;

public class TestQueryHistorySchema {

  @Test
  public void testSchemaFieldDataTypes() {
    testInspector("TEST_STRING_FIELD", true);
    testInspector("TEST_INT_FIELD", true);
    testInspector("TEST_BIGINT_FIELD", true);
    testInspector("TEST_TIMESTAMP_FIELD", true);

    testInspector("TEST_BOOLEAN_FIELD", false);
    testInspector("TEST_BYTE_FIELD", false);
    testInspector("TEST_SHORT_FIELD", false);
    testInspector("TEST_LONG_FIELD", false);
    testInspector("TEST_FLOAT_FIELD", false);
    testInspector("TEST_DOUBLE_FIELD", false);
    testInspector("TEST_VOID_FIELD", false);
    testInspector("TEST_DATE_FIELD", false);
  }

  private void testInspector(String fieldName, boolean supported) {
    FieldForTest field = FieldForTest.valueOf(fieldName);
    if (supported) {
      ObjectInspector inspector = QueryHistorySchema.createInspector(new FieldForTest[]{field});
      Assert.assertTrue("Main inspector should be a struct inspector, found " + inspector.getClass(),
          inspector instanceof StandardStructObjectInspector);
    } else {
      Assert.assertThrows(IllegalArgumentException.class, () -> {
        QueryHistorySchema.createInspector(new FieldForTest[]{field});
      });
    }
  }

  public enum FieldForTest implements QueryHistorySchema.QueryHistorySchemaField {
    TEST_STRING_FIELD("testString", "string", null),
    TEST_INT_FIELD("testInt", "int", null),
    TEST_BIGINT_FIELD("testBigint", "bigint", null),
    TEST_TIMESTAMP_FIELD("testTimestamp", "timestamp", null),
    TEST_BOOLEAN_FIELD("testBoolean", "boolean", null),
    TEST_BYTE_FIELD("testByte", "byte", null),
    TEST_SHORT_FIELD("testShort", "short", null),
    TEST_LONG_FIELD("testLong", "long", null),
    TEST_FLOAT_FIELD("testFloat", "float", null),
    TEST_DOUBLE_FIELD("testDouble", "double", null),
    TEST_VOID_FIELD("testVoid", "void", null),
    TEST_DATE_FIELD("testDate", "date", null);

    final String name;
    final String type;
    final String description;

    FieldForTest(String name, String type, String description) {
      this.name = name;
      this.type = type;
      this.description = description;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getType() {
      return type;
    }

    @Override
    public String getDescription() {
      return description;
    }

    @Override
    public boolean isPartitioningCol() {
      return false;
    }
  }
}
