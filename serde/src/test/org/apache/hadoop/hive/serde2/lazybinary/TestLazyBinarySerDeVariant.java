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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertReadsAroundVariantFields;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertReadsNestedVariantsAsEncodingStructs;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertRowInspectorIsShared;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.columns;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.init;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.row;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variantOI;
import static org.junit.Assert.assertSame;

public class TestLazyBinarySerDeVariant {

  private static byte[] serialize(Properties props, List<?> row, ObjectInspector rowOI) throws Exception {
    return ((BytesWritable) init(new LazyBinarySerDe(), props).serialize(row, rowOI)).copyBytes();
  }

  @Test
  public void testRowInspectorIsSharedAcrossSerDeInstances() throws Exception {
    assertRowInspectorIsShared(LazyBinarySerDe::new);
  }

  @Test
  public void testVariantInspectorIsCachedPerTypeInfo() throws Exception {
    TypeInfo variantType = TypeInfoFactory.getVariantTypeInfo();
    ObjectInspector oi = LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(variantType);
    assertSame(oi, LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(variantType));
    assertSame(LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(VariantEncoding.TYPE_INFO),
        ((StructEncodedVariantObjectInspector) oi).getEncodedObjectInspector());
    ListObjectInspector listOI = (ListObjectInspector) LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(
        TypeInfoFactory.getListTypeInfo(variantType));
    assertSame(oi, listOI.getListElementObjectInspector());
  }

  @Test
  public void testFastReaderKeepsPositionAroundVariantFields() throws Exception {
    Variant v = variant("{\"k\":5}");
    byte[] blob = serialize(columns("a,i,b", "variant,int,variant"), Arrays.asList(v, 42, null),
        row(List.of("a", "i", "b"), List.of(variantOI(), PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            variantOI())));

    LazyBinaryDeserializeRead reader = new LazyBinaryDeserializeRead(
        new TypeInfo[] {TypeInfoFactory.getVariantTypeInfo(), TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.getVariantTypeInfo()}, false);
    reader.set(blob, 0, blob.length);
    assertReadsAroundVariantFields(reader, v);
  }

  @Test
  public void testFastReaderReadsNestedVariantsAsEncodingStructs() throws Exception {
    Variant a = variant("1");
    Variant b = variant("{\"b\":2}");
    byte[] blob = serialize(columns("l,m", "array<variant>,map<string,variant>"),
        Arrays.asList(Arrays.asList(a, b), Map.of("k", b)), row(List.of("l", "m"), List.of(
            ObjectInspectorFactory.getStandardListObjectInspector(variantOI()),
            ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector, variantOI()))));

    TypeInfo variantType = TypeInfoFactory.getVariantTypeInfo();
    LazyBinaryDeserializeRead reader = new LazyBinaryDeserializeRead(
        new TypeInfo[] {TypeInfoFactory.getListTypeInfo(variantType),
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, variantType)}, false);
    reader.set(blob, 0, blob.length);
    assertReadsNestedVariantsAsEncodingStructs(reader, a, b);
  }
}
