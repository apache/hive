/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergVariantObjectInspector {

  private static org.apache.iceberg.variants.Variant toIceberg(Variant variant) {
    if (variant == null) {
      return null;
    }
    VariantMetadata metadata = VariantMetadata.from(
        ByteBuffer.wrap(variant.getMetadata()).order(ByteOrder.LITTLE_ENDIAN));
    return org.apache.iceberg.variants.Variant.of(metadata,
        VariantValue.from(metadata, ByteBuffer.wrap(variant.getValue()).order(ByteOrder.LITTLE_ENDIAN)));
  }

  @Test
  public void testCompareAndHashCodeAreTheStandardInspectorResults() throws Exception {
    ObjectInspector standard = ObjectInspectorFactory.getStandardVariantObjectInspector();
    ObjectInspector iceberg = IcebergVariantObjectInspector.get();
    ObjectInspector encoded = new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR);
    // value header 0xA1 and metadata bytes 0xC3 0xA9: bytes at or above 0x80
    List<Variant> values = Arrays.asList(null, parse("1"), parse("2"), parse("\"a\""), parse("{\"a\":1}"),
        parse("{\"b\":1}"), parse("\"\""), parse("{}"), parse("[]"), parse("\"" + "x".repeat(40) + "\""),
        parse("{\"é\":1}"), parse("{\"ab\":1}"));
    for (Variant a : values) {
      Assertions.assertEquals(ObjectInspectorUtils.hashCode(a, standard),
          ObjectInspectorUtils.hashCode(toIceberg(a), iceberg));
      for (Variant b : values) {
        int expected = ObjectInspectorUtils.compare(a, standard, b, standard);
        Assertions.assertEquals(expected, ObjectInspectorUtils.compare(toIceberg(a), iceberg, toIceberg(b), iceberg));
        Assertions.assertEquals(expected, ObjectInspectorUtils.compare(toIceberg(a), iceberg, b, standard));
        Assertions.assertEquals(expected, ObjectInspectorUtils.compare(toIceberg(a), iceberg,
            b == null ? null : VariantEncoding.fields(b), encoded));
      }
    }
  }

  private static Variant parse(String json) throws Exception {
    return VariantBuilder.parseJson(json, false);
  }
}
