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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.serde2;

import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.ENCODING;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSerDeUtils {

  private static ObjectInspector oi(String typeString) {
    return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
        TypeInfoUtils.getTypeInfoFromTypeString(typeString));
  }

  @Test
  public void testGetJSONStringRendersVariant() throws Exception {
    assertEquals("{\"age\":30,\"name\":\"John\"}",
        SerDeUtils.getJSONString(variant("{\"name\":\"John\",\"age\":30}"), oi("variant")));
  }

  @Test
  public void testGetJSONStringRendersNestedVariant() throws Exception {
    Object row = Arrays.asList("x", variant("{\"k\":1}"));
    assertEquals("{\"label\":\"x\",\"v\":{\"k\":1}}",
        SerDeUtils.getJSONString(row, oi("struct<label:string,v:variant>")));
  }

  @Test
  public void testGetJSONStringRendersVariantInListAndMap() throws Exception {
    Object list = Arrays.asList(variant("1"), null);
    assertEquals("[1,null]", SerDeUtils.getJSONString(list, oi("array<variant>")));

    Object map = Map.of("k", variant("{\"x\":true}"));
    assertEquals("{\"k\":{\"x\":true}}", SerDeUtils.getJSONString(map, oi("map<string,variant>")));
  }

  @Test
  public void testGetJSONStringRendersStructEncodedVariant() throws Exception {
    Variant v = variant("[1,\"a\"]");
    ObjectInspector encoded = new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR);
    assertEquals("[1,\"a\"]", SerDeUtils.getJSONString(VariantEncoding.fields(v), encoded));
  }

  @Test
  public void testNullVariant() {
    assertEquals("null", SerDeUtils.getJSONString(null, oi("variant")));
    assertEquals("NULL", SerDeUtils.getJSONString(null, oi("variant"), "NULL"));
  }

  @Test
  public void testStructOfTwoBinariesIsAStruct() throws Exception {
    // the variant encoding is only a variant under a VARIANT inspector
    String json = SerDeUtils.getJSONString(VariantEncoding.fields(variant("{\"k\":1}")),
        oi(ENCODING));
    assertTrue(json, json.startsWith("{\"metadata\":"));
  }

  @Test
  public void testMalformedVariantThrows() {
    Object malformed = Arrays.asList(new byte[] {2}, new byte[] {65});
    ObjectInspector encoded = new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR);
    RuntimeException e = Assert.assertThrows(RuntimeException.class,
        () -> SerDeUtils.getJSONString(malformed, encoded));
    assertEquals("MALFORMED_VARIANT", e.getMessage());
  }

  @Test
  public void testToThriftPayloadRendersVariant() throws Exception {
    assertEquals("{\"k\":1}", SerDeUtils.toThriftPayload(variant("{\"k\":1}"), oi("variant"), 10));
  }

  @Test
  public void testHasAnyNullObject() throws Exception {
    ObjectInspector oi = ObjectInspectorFactory.getStandardVariantObjectInspector();
    assertTrue(SerDeUtils.hasAnyNullObject(null, oi));
    // a variant null is a value, not SQL NULL
    assertFalse(SerDeUtils.hasAnyNullObject(variant("null"), oi));
  }

  @Test
  public void testThriftTypeForVariant() {
    assertEquals(Type.VARIANT_TYPE, Type.getType("variant"));
    assertEquals(Type.VARIANT_TYPE, Type.getType(TypeInfoUtils.getTypeInfoFromTypeString("variant")));
  }
}
