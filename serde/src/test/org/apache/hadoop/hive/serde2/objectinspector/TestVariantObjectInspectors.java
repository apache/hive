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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TestVariantObjectInspectors {

  /** A writable whose backing array is longer than its content, as serdes reuse them. */
  private static BytesWritable padded(byte[] bytes) {
    BytesWritable writable = new BytesWritable(bytes.clone());
    writable.setCapacity(bytes.length + 16);
    return writable;
  }

  @Test
  public void testStandardInspectorHoldsTheVariant() throws Exception {
    StandardVariantObjectInspector oi = ObjectInspectorFactory.getStandardVariantObjectInspector();
    assertSame(oi, ObjectInspectorFactory.getStandardVariantObjectInspector());
    assertSame(oi, ObjectInspectorFactory.getVariantObjectInspector());
    assertEquals(ObjectInspector.Category.VARIANT, oi.getCategory());
    assertEquals("variant", oi.getTypeName());
    // StructObjectInspector is an abstract class, so the static type must widen before instanceof compiles
    assertFalse(((ObjectInspector) oi) instanceof StructObjectInspector);

    Variant v = variant("{\"k\":1}");
    assertSame(v, oi.getVariant(v));
    assertNull(oi.getVariant(null));
  }

  @Test
  public void testTypeInfoUtilsReturnsTheStandardInspector() {
    TypeInfo variant = TypeInfoFactory.getVariantTypeInfo();
    assertSame(ObjectInspectorFactory.getStandardVariantObjectInspector(),
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(variant));
    assertSame(ObjectInspectorFactory.getStandardVariantObjectInspector(),
        TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(variant));
  }

  @Test
  public void testConstantInspector() throws Exception {
    Variant v = variant("[1,2]");
    StandardConstantVariantObjectInspector oi = ObjectInspectorFactory.getStandardConstantVariantObjectInspector(v);
    assertSame(v, oi.getWritableConstantValue());
    assertSame(v, oi.getVariant(v));
    assertEquals(ObjectInspector.Category.VARIANT, oi.getCategory());
    assertNull(ObjectInspectorFactory.getStandardConstantVariantObjectInspector(null).getWritableConstantValue());
  }

  @Test
  public void testStructEncodedInspectorCopiesTheExactBytes() throws Exception {
    Variant v = variant("{\"k\":\"abc\"}");
    BytesWritable metadata = padded(v.getMetadata());
    BytesWritable value = padded(v.getValue());
    StructObjectInspector encoded = ObjectInspectorFactory.getStandardStructObjectInspector(
        List.of(VariantEncoding.METADATA, VariantEncoding.VALUE),
        List.<ObjectInspector>of(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));
    StructEncodedVariantObjectInspector oi = new StructEncodedVariantObjectInspector(encoded);
    assertSame(encoded, oi.getEncodedObjectInspector());
    assertEquals(ObjectInspector.Category.VARIANT, oi.getCategory());
    assertEquals("variant", oi.getTypeName());

    Variant read = oi.getVariant(Arrays.asList(metadata, value));
    assertEquals(v, read);
    // serdes reuse their writables: later writes must not reach the variant already returned
    Arrays.fill(metadata.getBytes(), (byte) 0);
    Arrays.fill(value.getBytes(), (byte) 0);
    assertEquals("{\"k\":\"abc\"}", read.toJson(ZoneOffset.UTC));
    assertNull(oi.getVariant(null));
  }

  @Test
  public void testStructEncodedInspectorCopiesJavaBytes() throws Exception {
    Variant v = variant("true");
    byte[] metadata = v.getMetadata().clone();
    byte[] value = v.getValue().clone();
    Variant read = new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR)
        .getVariant(Arrays.asList(metadata, value));
    Arrays.fill(value, (byte) 0);
    assertEquals("true", read.toJson(ZoneOffset.UTC));
  }

  @Test
  public void testVariantEncoding() throws Exception {
    assertEquals("struct<metadata:binary,value:binary>", VariantEncoding.TYPE_INFO.getTypeName());
    assertEquals(VariantEncoding.TYPE_INFO.getTypeName(), VariantEncoding.JAVA_OBJECT_INSPECTOR.getTypeName());
    Variant v = variant("{\"a\":[1]}");
    List<Object> fields = VariantEncoding.fields(v);
    assertSame(v.getMetadata(), fields.get(0));
    assertSame(v.getValue(), fields.get(1));
  }
}
