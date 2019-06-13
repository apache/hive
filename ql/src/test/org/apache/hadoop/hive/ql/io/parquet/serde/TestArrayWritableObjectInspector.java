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
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.Arrays;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;

import junit.framework.TestCase;

/**
 * Tests for ArrayWritableObjectInspector. At the moment only behavior related to HIVE-21796 covered.
 */
public class TestArrayWritableObjectInspector extends TestCase {

  private StructTypeInfo nestOnce(TypeInfo nestedType) {
    return (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(Arrays.asList("value"), Arrays.asList(nestedType));
  }

  private StructTypeInfo createNestedStruct(TypeInfo nestedType, int depth) {
    StructTypeInfo result = nestOnce(nestedType);
    for(int i = 0; i < depth; i++){
      result = nestOnce(result);
    }
    return result;
  }

  /** Regression tests for HIVE-21796: equals and hash takes forever if HIVE-21796 is reverted / reintroduced. */
  public void testIdenticalInspectorsEquals() {
    StructTypeInfo nestedStruct = createNestedStruct(TypeInfoFactory.getPrimitiveTypeInfo("int"), 1000);
    ArrayWritableObjectInspector inspectorX = new ArrayWritableObjectInspector(true, nestedStruct, null);
    ArrayWritableObjectInspector inspectorY = new ArrayWritableObjectInspector(true, nestedStruct, null);
    Assert.assertEquals(inspectorX, inspectorY);
    Assert.assertEquals(inspectorX.hashCode(), inspectorY.hashCode());
  }

  /** Regression tests for HIVE-21796: equals and hash takes forever if HIVE-21796 is reverted / reintroduced. */
  public void testEqualInspectorsEquals() {
    StructTypeInfo nestedStructX = createNestedStruct(TypeInfoFactory.getPrimitiveTypeInfo("int"), 100);
    StructTypeInfo nestedStructY = createNestedStruct(TypeInfoFactory.getPrimitiveTypeInfo("int"), 100);
    ArrayWritableObjectInspector inspectorX = new ArrayWritableObjectInspector(true, nestedStructX, null);
    ArrayWritableObjectInspector inspectorY = new ArrayWritableObjectInspector(true, nestedStructY, null);
    Assert.assertEquals(inspectorX, inspectorY);
    Assert.assertEquals(inspectorX.hashCode(), inspectorY.hashCode());
  }

  /** Regression tests for HIVE-21796: equals and hash takes forever if HIVE-21796 is reverted / reintroduced. */
  public void testDifferentInspectorsEquals() {
    StructTypeInfo nestedStructX = createNestedStruct(TypeInfoFactory.getPrimitiveTypeInfo("int"), 100);
    StructTypeInfo nestedStructY = createNestedStruct(TypeInfoFactory.getPrimitiveTypeInfo("bigint"), 100);
    ArrayWritableObjectInspector inspectorX = new ArrayWritableObjectInspector(true, nestedStructX, null);
    ArrayWritableObjectInspector inspectorY = new ArrayWritableObjectInspector(true, nestedStructY, null);
    Assert.assertNotEquals(inspectorX, inspectorY);
    Assert.assertNotEquals(inspectorX.hashCode(), inspectorY.hashCode());
  }

}
