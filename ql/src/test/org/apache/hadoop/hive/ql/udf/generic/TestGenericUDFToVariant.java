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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestGenericUDFToVariant {

  private static String toJson(String typeString, Object value) throws Exception {
    return toJson(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
        TypeInfoUtils.getTypeInfoFromTypeString(typeString)), value);
  }

  private static String toJson(ObjectInspector oi, Object value) throws Exception {
    GenericUDFToVariant udf = new GenericUDFToVariant();
    udf.initialize(new ObjectInspector[] { oi });
    Object result = udf.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(value) });
    return result == null ? null : ((Variant) result).toJson(ZoneOffset.UTC);
  }

  @Test
  public void testPrimitives() throws Exception {
    assertEquals("true", toJson("boolean", true));
    assertEquals("42", toJson("int", 42));
    assertEquals("9007199254740993", toJson("bigint", 9007199254740993L));
    assertEquals("1.5", toJson("double", 1.5d));
    assertEquals("1.5", toJson("decimal(4,2)", HiveDecimal.create("1.50")));
    assertEquals("\"John\"", toJson("string", "John"));
    assertEquals("\"ab\"", toJson("varchar(5)", new HiveVarchar("ab", 5)));
    // char padding is stripped, consistent with Hive's char-to-string coercion
    assertEquals("\"ab\"", toJson("char(5)", new HiveChar("ab", 5)));
    assertNull(toJson("int", null));
  }

  @Test
  public void testBinaryBecomesTypedNode() throws Exception {
    // base64 of DEADBEEF; a real BINARY node, unreachable via parse_json
    assertEquals("\"3q2+7w==\"", toJson("binary", new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF }));
  }

  @Test
  public void testDateAndTimestamp() throws Exception {
    assertEquals("\"2026-07-28\"", toJson("date", Date.valueOf("2026-07-28")));
    assertEquals("\"2026-07-28 10:00:00\"", toJson("timestamp", Timestamp.valueOf("2026-07-28 10:00:00")));
  }

  @Test
  public void testArrayWithNulls() throws Exception {
    assertEquals("[1,null,3]", toJson("array<int>", Arrays.asList(1, null, 3)));
  }

  @Test
  public void testVariantInputIsEmbeddedAsIs() throws Exception {
    Variant v = VariantBuilder.parseJson("{\"k\":1}", false);
    assertEquals("{\"k\":1}", toJson("variant", v));
    assertEquals("{\"k\":1}", toJson(new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR),
        VariantEncoding.fields(v)));
  }

  @Test
  public void testStructAndMapRejected() {
    assertThrows(UDFArgumentTypeException.class, () -> toJson("struct<a:int>", null));
    assertThrows(UDFArgumentTypeException.class, () -> toJson("map<string,int>", null));
    // the layout a variant used to be is an ordinary struct now
    assertThrows(UDFArgumentTypeException.class, () -> toJson("struct<metadata:binary,value:binary>", null));
  }

  @Test
  public void testMalformedVariantInputThrows() {
    // strict like parse_json and Spark's cast: bytes that are not variant encoding fail loudly
    assertThrows(HiveException.class,
        () -> toJson(new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR),
            Arrays.asList(new byte[] { 9 }, new byte[] { 1 })));
  }
}
