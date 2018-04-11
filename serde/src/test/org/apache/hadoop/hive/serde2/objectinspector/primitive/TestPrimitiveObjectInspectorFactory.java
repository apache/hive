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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public class TestPrimitiveObjectInspectorFactory extends TestCase {

  public void testGetPrimitiveWritableObjectInspector() {
    // even without type params, return a default OI for varchar
    PrimitiveObjectInspector poi = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(PrimitiveCategory.VARCHAR);
    assertEquals(poi, PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector);

    // Same for char
    poi = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(PrimitiveCategory.CHAR);
    assertEquals(poi, PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector);
  }

  public void testGetPrimitiveJavaObjectInspector() {
    // even without type params, return a default OI for varchar
    PrimitiveObjectInspector poi = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VARCHAR);
    assertEquals(poi, PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector);

    // same for char
    poi = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.CHAR);
    assertEquals(poi, PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector);
  }
}
