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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.ENCODING;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.assertReadsAroundVariantFields;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.columns;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.init;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.row;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variantOI;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestBinarySortableSerDeVariant {

  /** Serializes a row of the given columns as a key, with nulls first in every column. */
  private static byte[] key(String names, String types, String order, List<?> row, List<ObjectInspector> fieldOIs)
      throws Exception {
    Properties props = columns(names, types);
    props.setProperty(serdeConstants.SERIALIZATION_SORT_ORDER, order);
    props.setProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER, "a".repeat(order.length()));
    return ((BytesWritable) init(new BinarySortableSerDe(), props)
        .serialize(row, row(List.of(names.split(",")), fieldOIs))).copyBytes();
  }

  private static byte[] key(Variant v, String order) throws Exception {
    return key("k", "variant", order, Arrays.asList(v), List.of(variantOI()));
  }

  private static byte[] structKey(Variant v, String order) throws Exception {
    Object struct = v == null ? null : VariantEncoding.fields(v);
    return key("k", ENCODING, order, Arrays.asList(struct), List.of(VariantEncoding.JAVA_OBJECT_INSPECTOR));
  }

  @Test
  public void testKeysAreTheStructKeysSoTheyOrderTheSameIncludingNull() throws Exception {
    Variant[] variants = {null, variant("null"), variant("1"), variant("2"), variant("\"s\""),
        variant("{\"a\":1}"), variant("{\"a\":2}"), variant("{\"b\":1}"), variant("[1,2]")};
    for (String order : List.of("+", "-")) {
      byte[][] keys = new byte[variants.length][];
      for (int i = 0; i < variants.length; i++) {
        keys[i] = key(variants[i], order);
        assertArrayEquals(order + " #" + i, structKey(variants[i], order), keys[i]);
      }
      // the SQL NULL key sorts first (null order "a"), ahead of every variant
      for (int i = 1; i < variants.length; i++) {
        assertEquals(order + " null vs #" + i, -1, Integer.signum(
            WritableComparator.compareBytes(keys[0], 0, keys[0].length, keys[i], 0, keys[i].length)));
      }
    }
  }

  @Test
  public void testFastReaderKeepsPositionAroundVariantFields() throws Exception {
    Variant v = variant("{\"k\":5}");
    byte[] blob = key("a,i,b", "variant,int,variant", "+++", Arrays.asList(v, 42, null),
        List.of(variantOI(), PrimitiveObjectInspectorFactory.javaIntObjectInspector, variantOI()));

    BinarySortableDeserializeRead reader = BinarySortableDeserializeRead.ascendingNullsFirst(
        new TypeInfo[] {TypeInfoFactory.getVariantTypeInfo(), TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.getVariantTypeInfo()}, false);
    reader.set(blob, 0, blob.length);
    assertReadsAroundVariantFields(reader, v);
  }
}
