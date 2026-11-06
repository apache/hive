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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.GeospatialTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGenericUDFToGeometry {

  private static final byte[] WKB_POINT = new byte[] {
      0x01, 0x01, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xf0, 0x3f,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 };

  private static GenericUDFToGeometry udf(String crs) throws UDFArgumentException {
    GenericUDFToGeometry udf = new GenericUDFToGeometry();
    udf.setTypeInfo(TypeInfoFactory.getGeometryTypeInfo(crs));
    return udf;
  }

  private static ObjectInspector geometryInspector(String crs) {
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
        TypeInfoFactory.getGeometryTypeInfo(crs));
  }

  @Test
  public void testBinaryToGeometry() throws Exception {
    GenericUDFToGeometry udf = udf("EPSG:3857");
    PrimitiveObjectInspector output = (PrimitiveObjectInspector) udf.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector });
    assertEquals(TypeInfoFactory.getGeometryTypeInfo("EPSG:3857"), output.getTypeInfo());
    assertEquals(new BytesWritable(WKB_POINT),
        udf.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(WKB_POINT) }));
  }

  @Test
  public void testSameCrs() throws Exception {
    GenericUDFToGeometry udf = udf("EPSG:3857");
    PrimitiveObjectInspector output =
        (PrimitiveObjectInspector) udf.initialize(new ObjectInspector[] { geometryInspector("epsg:3857") });
    assertEquals("geometry('EPSG:3857')", output.getTypeName());
    assertEquals(new BytesWritable(WKB_POINT), udf.evaluate(
        new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(new BytesWritable(WKB_POINT)) }));

    udf(GeospatialTypeInfo.DEFAULT_CRS).initialize(new ObjectInspector[] {
        PrimitiveObjectInspectorFactory.writableGeometryObjectInspector });
  }

  @Test
  public void testDifferentCrsIsRejected() {
    UDFArgumentException e = assertThrows(UDFArgumentException.class,
        () -> udf("EPSG:3857").initialize(new ObjectInspector[] { geometryInspector("EPSG:4326") }));
    assertTrue(e.getMessage().contains("geometry('EPSG:4326')"), e.getMessage());
    assertTrue(e.getMessage().contains("geometry('EPSG:3857')"), e.getMessage());

    assertThrows(UDFArgumentException.class, () -> udf("EPSG:3857").initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableGeometryObjectInspector }));
  }

  @Test
  public void testInvalidWkbIsRejected() throws Exception {
    GenericUDFToGeometry udf = udf("EPSG:3857");
    udf.initialize(new ObjectInspector[] { PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector });
    byte[] ewkb = new byte[] {0x01, 0x01, 0x00, 0x00, 0x20, (byte) 0xe6, 0x10, 0x00, 0x00};
    for (byte[] invalid : new byte[][] {new byte[] {0x01, 0x01}, ewkb, "POINT (1 2)".getBytes(UTF_8)}) {
      HiveException e = assertThrows(HiveException.class,
          () -> udf.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(invalid) }));
      assertTrue(e.getMessage().startsWith("Cannot cast binary to geometry('EPSG:3857'): Invalid WKB: "),
          e.getMessage());
    }
  }

  @Test
  public void testGeometryInputIsNotRevalidated() throws Exception {
    GenericUDFToGeometry udf = udf(GeospatialTypeInfo.DEFAULT_CRS);
    udf.initialize(new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableGeometryObjectInspector });
    BytesWritable stored = new BytesWritable(new byte[] {0x01});
    assertEquals(stored, udf.evaluate(
        new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(stored) }));
  }

  @Test
  public void testStringIsRejected() {
    assertThrows(UDFArgumentException.class, () -> udf(GeospatialTypeInfo.DEFAULT_CRS).initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.javaStringObjectInspector }));
  }
}
