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
package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * A merge join compares VARIANT keys that the shuffle sorted by their BinarySortable bytes, so the comparator must
 * give the same order.
 */
public class TestHiveVariantComparator {
  private static final byte[] PRIMITIVE_METADATA = {0x01, 0x00, 0x00};

  private static Variant json(String json) throws Exception {
    return VariantBuilder.parseJson(json, false);
  }

  private static String hex(Variant v) {
    HexFormat hex = HexFormat.of();
    return "metadata=" + hex.formatHex(v.getMetadata()) + " value=" + hex.formatHex(v.getValue());
  }

  /**
   * Covers both fields deciding the order, bytes >= 0x80 and proper prefixes: '200' sorts before '{"a":1}' by
   * metadata but after it by value; '-5' (0C FB) sorts after '1' (0C 01) only when unsigned; a value padded by a
   * trailing byte, as Hive 4.2.x wrote to Iceberg files, and a longer metadata sort after their prefix.
   */
  private static List<Variant> variants() throws Exception {
    return List.of(json("{\"a\":1}"), json("200"), json("-200"), json("1"), json("-5"), json("\"\u00e9\""),
        json("\"e\""), json("{\"b\":[1,2]}"), json("[1,2,3]"), json("null"),
        new Variant(new byte[] {0x0C, 0x01, 0x00}, PRIMITIVE_METADATA),
        new Variant(new byte[] {0x0C, 0x01}, new byte[] {0x01, 0x00, 0x00, 0x00}));
  }

  private static byte[] sortKey(Variant v, String order) throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "k");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "variant");
    props.setProperty(serdeConstants.SERIALIZATION_SORT_ORDER, order);
    props.setProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER, "a");
    BinarySortableSerDe serde = new BinarySortableSerDe();
    serde.initialize(new Configuration(), props, null);
    ObjectInspector rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(List.of("k"),
        List.of(ObjectInspectorFactory.getStandardVariantObjectInspector()));
    return ((BytesWritable) serde.serialize(Arrays.asList(v), rowOI)).copyBytes();
  }

  private static int compareKeys(Variant v1, Variant v2, String order) throws Exception {
    byte[] k1 = sortKey(v1, order);
    byte[] k2 = sortKey(v2, order);
    return Integer.signum(WritableComparator.compareBytes(k1, 0, k1.length, k2, 0, k2.length));
  }

  @Test
  public void testOrderIsTheBinarySortableKeyOrder() throws Exception {
    List<Variant> variants = variants();
    WritableComparator comparator = WritableComparatorFactory.get(variants.get(0), false, NullOrdering.NULLS_FIRST);

    List<Variant> byComparator = new ArrayList<>(variants);
    byComparator.sort(comparator::compare);
    List<Variant> byKey = new ArrayList<>(variants);
    byKey.sort((v1, v2) -> {
      try {
        return compareKeys(v1, v2, "+");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    assertEquals(byKey.stream().map(TestHiveVariantComparator::hex).toList(),
        byComparator.stream().map(TestHiveVariantComparator::hex).toList());

    ObjectInspector oi = ObjectInspectorFactory.getStandardVariantObjectInspector();
    for (Variant v1 : variants) {
      for (Variant v2 : variants) {
        int expected = compareKeys(v1, v2, "+");
        String pair = hex(v1) + " vs " + hex(v2);
        assertEquals(pair, expected, Integer.signum(comparator.compare(v1, v2)));
        assertEquals(pair, expected, Integer.signum(ObjectInspectorUtils.compare(v1, oi, v2, oi)));
        // descending keys sort in the reverse order
        assertEquals(pair, -expected, compareKeys(v1, v2, "-"));
      }
    }
  }

  @Test
  public void testEqualEncodingsCompareEqual() throws Exception {
    Variant v = json("{\"a\":[1,\"x\"]}");
    Variant copy = new Variant(v.getValue().clone(), v.getMetadata().clone());
    assertEquals(0, WritableComparatorFactory.get(v, false, null).compare(v, copy));
  }

  @Test
  public void testNullsFollowTheNullOrdering() throws Exception {
    Object v = json("1");
    Object none = null;
    for (NullOrdering ordering : NullOrdering.values()) {
      for (boolean nullSafe : new boolean[] {false, true}) {
        WritableComparator variants = WritableComparatorFactory.get(v, nullSafe, ordering);
        WritableComparator writables = new HiveWritableComparator(nullSafe, ordering);
        assertEquals(writables.compare(none, v), variants.compare(none, v));
        assertEquals(writables.compare(v, none), variants.compare(v, none));
        assertEquals(writables.compare(none, none), variants.compare(none, none));
      }
    }
  }
}
