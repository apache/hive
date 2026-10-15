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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.MultiDelimitSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.ENCODING;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertReadsAroundVariantFields;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertReadsNestedVariantsAsEncodingStructs;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertRowInspectorIsShared;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.columns;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.init;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.read;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.row;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variantOI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestLazySimpleSerDeVariant {

  private static LazySimpleDeserializeRead fastReader(Properties props, TypeInfo... typeInfos)
      throws SerDeException {
    return new LazySimpleDeserializeRead(typeInfos, false,
        new LazySerDeParameters(new Configuration(), props, LazySimpleSerDe.class.getName()));
  }

  @Test
  public void testNullVariant() throws Exception {
    LazySimpleSerDe serde = init(new LazySimpleSerDe(), columns("v", "variant"));
    Writable blob = serde.serialize(Arrays.asList((Object) null), row(List.of("v"), List.of(variantOI())));
    assertEquals(new Text("\\N"), blob);
    StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
    assertNull(read(rowOI, serde.deserialize(blob), 0));
  }

  @Test
  public void testFastReaderKeepsPositionAroundVariantFields() throws Exception {
    Variant v = variant("{\"k\":5}");
    Properties props = columns("a,i,b", "variant,int,variant");
    Text text = (Text) init(new LazySimpleSerDe(), props).serialize(Arrays.asList(v, 42, null),
        row(List.of("a", "i", "b"), List.of(variantOI(), PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            variantOI())));

    LazySimpleDeserializeRead reader = fastReader(props, TypeInfoFactory.getVariantTypeInfo(),
        TypeInfoFactory.intTypeInfo, TypeInfoFactory.getVariantTypeInfo());
    reader.set(text.getBytes(), 0, text.getLength());
    assertReadsAroundVariantFields(reader, v);
  }

  @Test
  public void testFastReaderReadsNestedVariantsAsEncodingStructs() throws Exception {
    Variant a = variant("1");
    Variant b = variant("{\"b\":2}");
    Properties props = columns("l,m", "array<variant>,map<string,variant>");
    Text text = (Text) init(new LazySimpleSerDe(), props).serialize(
        Arrays.asList(Arrays.asList(a, b), Map.of("k", b)), row(List.of("l", "m"), List.of(
            ObjectInspectorFactory.getStandardListObjectInspector(variantOI()),
            ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector, variantOI()))));

    TypeInfo variantType = TypeInfoFactory.getVariantTypeInfo();
    LazySimpleDeserializeRead reader = fastReader(props, TypeInfoFactory.getListTypeInfo(variantType),
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, variantType));
    reader.set(text.getBytes(), 0, text.getLength());
    assertReadsNestedVariantsAsEncodingStructs(reader, a, b);
  }

  @Test
  public void testRowInspectorIsSharedAcrossSerDeInstances() throws Exception {
    assertRowInspectorIsShared(LazySimpleSerDe::new);
  }

  @Test
  public void testMultiDelimitSerDeWritesTheStructBytes() throws Exception {
    Variant v = variant("{\"k\":3}");
    Properties variantProps = columns("i,v", "int,variant");
    variantProps.setProperty(serdeConstants.FIELD_DELIM, "|+|");
    Properties structProps = columns("i,v", "int," + ENCODING);
    structProps.setProperty(serdeConstants.FIELD_DELIM, "|+|");
    ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    Writable asVariant = init(new MultiDelimitSerDe(), variantProps)
        .serialize(Arrays.asList(1, v), row(List.of("i", "v"), List.of(intOI, variantOI())));
    Writable asStruct = init(new MultiDelimitSerDe(), structProps)
        .serialize(Arrays.asList(1, VariantEncoding.fields(v)),
            row(List.of("i", "v"), List.of(intOI, VariantEncoding.JAVA_OBJECT_INSPECTOR)));
    assertEquals(asStruct, asVariant);
  }
}
