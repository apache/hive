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
package org.apache.hadoop.hive.serde2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSerDeUtils {

  private static ObjectInspector oi(String typeString) {
    return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
        TypeInfoUtils.getTypeInfoFromTypeString(typeString));
  }

  private static List<Object> variant(String json) throws Exception {
    Variant v = VariantBuilder.parseJson(json, false);
    return List.of(v.getMetadata(), v.getValue());
  }

  @Test
  public void testGetJSONStringDecodesVariant() throws Exception {
    String json = SerDeUtils.getJSONString(variant("{\"name\":\"John\",\"age\":30}"),
        oi("struct<metadata:binary,value:binary>"), "null", true);
    assertEquals("{\"age\":30,\"name\":\"John\"}", json);
  }

  @Test
  public void testGetJSONStringDecodesNestedVariant() throws Exception {
    Object row = Arrays.asList("x", variant("{\"k\":1}"));
    String json = SerDeUtils.getJSONString(row, oi("struct<label:string,v:variant>"), "null", true);
    assertEquals("{\"label\":\"x\",\"v\":{\"k\":1}}", json);
  }

  @Test
  public void testGetJSONStringDecodesVariantInListAndMap() throws Exception {
    Object list = Arrays.asList(variant("1"), null);
    assertEquals("[1,null]", SerDeUtils.getJSONString(list, oi("array<variant>"), "null", true));

    Object map = Map.of("k", variant("{\"x\":true}"));
    assertEquals("{\"k\":{\"x\":true}}", SerDeUtils.getJSONString(map, oi("map<string,variant>"), "null", true));
  }

  @Test
  public void testNonVariantBytesKeepGenericRendering() throws Exception {
    Object row = Arrays.asList(new byte[] { 2 }, new byte[] { 65 });
    String json = SerDeUtils.getJSONString(row, oi("struct<metadata:binary,value:binary>"), "null", true);
    assertEquals("{\"metadata\":\u0002,\"value\":A}", json);
  }

  @Test
  public void testNullVariantFieldsKeepGenericRendering() throws Exception {
    Object row = Arrays.asList(null, null);
    String json = SerDeUtils.getJSONString(row, oi("struct<metadata:binary,value:binary>"), "null", true);
    assertEquals("{\"metadata\":null,\"value\":null}", json);
  }

  @Test
  public void testVariantIsNotDecodedByDefault() throws Exception {
    String json = SerDeUtils.getJSONString(variant("{\"k\":1}"), oi("struct<metadata:binary,value:binary>"));
    assertTrue(json, json.startsWith("{\"metadata\":"));
  }

  @Test
  public void testToThriftPayloadDecodesVariant() throws Exception {
    Object payload = SerDeUtils.toThriftPayload(variant("{\"k\":1}"), oi("struct<metadata:binary,value:binary>"), 10);
    assertEquals("{\"k\":1}", payload);
  }

  @Test
  public void testThriftTypeForVariant() {
    assertEquals(Type.VARIANT_TYPE, Type.getType("variant"));
    assertEquals(Type.VARIANT_TYPE, Type.getType(TypeInfoUtils.getTypeInfoFromTypeString("variant")));
  }
}