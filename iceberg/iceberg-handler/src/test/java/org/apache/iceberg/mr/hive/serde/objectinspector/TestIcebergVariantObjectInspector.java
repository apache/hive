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

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantUtil;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergVariantObjectInspector {

  @Test
  public void testTimeAndNanosTimestampsWrittenByIceberg() {
    VariantMetadata metadata = Variants.metadata("t", "ts", "ntz");
    ShreddedObject object = Variants.object(metadata);
    object.put("t", Variants.ofIsoTime("13:45:30.123456"));
    object.put("ts", Variants.ofIsoTimestamptzNanos("2024-01-15T13:45:30.123456789+00:00"));
    ValueArray array = Variants.array();
    array.add(Variants.ofIsoTimestampntzNanos("1969-12-31T23:59:59.876543211"));
    object.put("ntz", array);
    org.apache.iceberg.variants.Variant iceberg = org.apache.iceberg.variants.Variant.of(metadata, object);
    StructObjectInspector oi = (StructObjectInspector) IcebergVariantObjectInspector.get();

    Variant hive = Variant.from(oi.getStructFieldsDataAsList(iceberg));

    Variant time = hive.getFieldByKey("t");
    Assertions.assertEquals(VariantUtil.Type.TIME, time.getType());
    Assertions.assertEquals(49_530_123_456L, time.getLong());
    Variant nanos = hive.getFieldByKey("ts");
    Assertions.assertEquals(VariantUtil.Type.TIMESTAMP_NANOS, nanos.getType());
    Assertions.assertEquals(1_705_326_330_123_456_789L, nanos.getLong());
    Variant ntzNanos = hive.getFieldByKey("ntz").getElementAtIndex(0);
    Assertions.assertEquals(VariantUtil.Type.TIMESTAMP_NTZ_NANOS, ntzNanos.getType());
    Assertions.assertEquals(-123_456_789L, ntzNanos.getLong());
    Assertions.assertEquals("{\"ntz\":[\"1969-12-31 23:59:59.876543211\"],\"t\":\"13:45:30.123456\"," +
        "\"ts\":\"2024-01-15 13:45:30.123456789+00:00\"}",
        SerDeUtils.getJSONString(iceberg, oi, "null", true));
  }
}
