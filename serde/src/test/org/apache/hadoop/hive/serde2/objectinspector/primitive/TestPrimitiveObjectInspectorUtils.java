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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

import junit.framework.TestCase;

public class TestPrimitiveObjectInspectorUtils extends TestCase {

  public void testGetPrimitiveGrouping() {
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BYTE));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.SHORT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.INT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.LONG));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.FLOAT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DOUBLE));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DECIMAL));

    assertEquals(PrimitiveGrouping.STRING_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.STRING));

    assertEquals(PrimitiveGrouping.DATE_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DATE));
    assertEquals(PrimitiveGrouping.DATE_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.TIMESTAMP));

    assertEquals(PrimitiveGrouping.BOOLEAN_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BOOLEAN));

    assertEquals(PrimitiveGrouping.BINARY_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BINARY));

    assertEquals(PrimitiveGrouping.UNKNOWN_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.UNKNOWN));
    assertEquals(PrimitiveGrouping.VOID_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.VOID));
  }
}
