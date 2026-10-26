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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantUtil.TIME;
import static org.apache.hadoop.hive.serde2.variant.VariantUtil.TIMESTAMP_NANOS;
import static org.apache.hadoop.hive.serde2.variant.VariantUtil.TIMESTAMP_NTZ_NANOS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestGenericUDFVariantGet {

  // A primitive variant with an 8-byte little-endian payload and an empty dictionary, as variant struct data.
  private static List<Object> primitive(int typeId, long payload) {
    byte[] value = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
        .put((byte) (typeId << 2)).putLong(payload).array();
    return List.of(new byte[] {0x01, 0x00, 0x00}, value);
  }

  private static Object getString(GenericUDFVariantGet get, Object variant) throws Exception {
    return get.evaluate(new GenericUDF.DeferredObject[] {
        new GenericUDF.DeferredJavaObject(variant), new GenericUDF.DeferredJavaObject("$") });
  }

  @Test
  void variantGetRendersTimeAndNanosTimestampsAsStrings() throws Exception {
    ObjectInspector stringType = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("string"));
    GenericUDFVariantGet get = new GenericUDFVariantGet();
    get.initialize(new ObjectInspector[] {
        VariantObjectInspector.get(), PrimitiveObjectInspectorFactory.javaStringObjectInspector, stringType });

    assertEquals("13:45:30.123456", getString(get, primitive(TIME, 49_530_123_456L)));
    assertEquals("00:00:00", getString(get, primitive(TIME, 0)));
    assertEquals("2024-01-15T13:45:30.123456789Z",
        getString(get, primitive(TIMESTAMP_NANOS, 1_705_326_330_123_456_789L)));
    assertEquals("1969-12-31T23:59:59.876543211Z", getString(get, primitive(TIMESTAMP_NTZ_NANOS, -123_456_789L)));
  }
}
