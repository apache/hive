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

package org.apache.hadoop.hive.serde2.typeinfo;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class TestGeometryTypeInfo {

  private static final byte[] WKB_POINT = {
      0x01, 0x01, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xf0, 0x3f,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 };

  @Test
  public void testParseDefaultCrs() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.GEOMETRY_TYPE_NAME);
    Assert.assertSame(TypeInfoFactory.geometryTypeInfo, typeInfo);
    GeometryTypeInfo geometry = (GeometryTypeInfo) typeInfo;
    Assert.assertEquals(PrimitiveCategory.GEOMETRY, geometry.getPrimitiveCategory());
    Assert.assertEquals(GeospatialTypeInfo.DEFAULT_CRS, geometry.getCrs());
    Assert.assertEquals("geometry", geometry.getTypeName());
    Assert.assertEquals(byte[].class, geometry.getPrimitiveJavaClass());
    Assert.assertEquals(BytesWritable.class, geometry.getPrimitiveWritableClass());
  }

  @Test
  public void testParseCrs() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("geometry('EPSG:3857')");
    Assert.assertEquals(new GeometryTypeInfo("EPSG:3857"), typeInfo);
    Assert.assertEquals("EPSG:3857", ((GeometryTypeInfo) typeInfo).getCrs());
    Assert.assertEquals("geometry('EPSG:3857')", typeInfo.getTypeName());
    Assert.assertEquals(typeInfo, TypeInfoFactory.getPrimitiveTypeInfo("geometry('EPSG:3857')"));
    Assert.assertEquals(typeInfo, TypeInfoFactory.getGeometryTypeInfo("EPSG:3857"));
  }

  @Test
  public void testExplicitDefaultCrsIsNormalized() {
    Assert.assertEquals(TypeInfoFactory.geometryTypeInfo,
        TypeInfoUtils.getTypeInfoFromTypeString("geometry('OGC:CRS84')"));
    Assert.assertEquals("geometry", TypeInfoFactory.getGeometryTypeInfo("OGC:CRS84").getTypeName());
    Assert.assertEquals(TypeInfoFactory.geometryTypeInfo, TypeInfoFactory.getGeometryTypeInfo(null));
    Assert.assertEquals("geometry", TypeInfoFactory.getGeometryTypeInfo("ogc:crs84").getTypeName());
    Assert.assertEquals(GeospatialTypeInfo.DEFAULT_CRS, new GeometryTypeInfo("ogc:crs84").getCrs());
    Assert.assertEquals(TypeInfoFactory.geometryTypeInfo, new GeometryTypeInfo("ogc:crs84"));
  }

  @Test
  public void testCrsSpellingIsKept() {
    GeometryTypeInfo upper = TypeInfoFactory.getGeometryTypeInfo("EPSG:3857");
    GeometryTypeInfo lower = TypeInfoFactory.getGeometryTypeInfo("epsg:3857");
    Assert.assertNotEquals(upper, lower);
    Assert.assertEquals("epsg:3857", lower.getCrs());
    Assert.assertEquals("geometry('epsg:3857')", PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(lower).getTypeName());
    Assert.assertEquals("geometry('EPSG:3857')", PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(upper).getTypeName());
  }

  @Test
  public void testCrsCompatibilityIgnoresCase() {
    GeometryTypeInfo upper = TypeInfoFactory.getGeometryTypeInfo("EPSG:3857");
    GeometryTypeInfo lower = TypeInfoFactory.getGeometryTypeInfo("epsg:3857");
    Assert.assertTrue(upper.isCompatibleWith(lower));
    Assert.assertTrue(lower.isCompatibleWith(upper));
    Assert.assertFalse(upper.isCompatibleWith(TypeInfoFactory.geometryTypeInfo));
    Assert.assertFalse(upper.isCompatibleWith(TypeInfoFactory.binaryTypeInfo));
    Assert.assertFalse(upper.isCompatibleWith(null));
  }

  @Test
  public void testCrsDistinguishesTypes() {
    Assert.assertNotEquals(TypeInfoFactory.geometryTypeInfo, TypeInfoFactory.getGeometryTypeInfo("EPSG:3857"));
    Assert.assertNotEquals(TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.geometryTypeInfo);
  }

  @Test
  public void testParseInsideComplexTypes() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(
        "int,geometry('srid:3857'),struct<g:geometry('EPSG:4326'),b:binary>,array<geometry>");
    Assert.assertEquals(4, typeInfos.size());
    Assert.assertEquals(TypeInfoFactory.getGeometryTypeInfo("srid:3857"), typeInfos.get(1));
    StructTypeInfo struct = (StructTypeInfo) typeInfos.get(2);
    Assert.assertEquals(TypeInfoFactory.getGeometryTypeInfo("EPSG:4326"), struct.getStructFieldTypeInfo("g"));
    Assert.assertEquals(TypeInfoFactory.binaryTypeInfo, struct.getStructFieldTypeInfo("b"));
    Assert.assertEquals(TypeInfoFactory.geometryTypeInfo,
        ((ListTypeInfo) typeInfos.get(3)).getListElementTypeInfo());
    Assert.assertEquals("struct<g:geometry('EPSG:4326'),b:binary>", struct.getTypeName());
  }

  @Test
  public void testQuotedFirstParameterFollowedByAnotherParameter() {
    TypeInfoUtils.PrimitiveParts parts = TypeInfoUtils.parsePrimitiveParts("geometry('EPSG:4326',spherical)");
    Assert.assertArrayEquals(new String[] {"EPSG:4326", "spherical"}, parts.typeParams);
  }

  @Test
  public void testInvalidCrs() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new GeometryTypeInfo(" "));
    Assert.assertThrows(IllegalArgumentException.class, () -> new GeometryTypeInfo("a'b"));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> TypeInfoUtils.getTypeInfoFromTypeString("geometry('EPSG:4326',spherical)"));
  }

  @Test
  public void testBinaryClassesStillResolveToBinary() {
    Assert.assertEquals(TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.getPrimitiveTypeInfoFromPrimitiveWritable(BytesWritable.class));
    Assert.assertEquals(TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(byte[].class));
    Assert.assertEquals(PrimitiveCategory.GEOMETRY,
        PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(serdeConstants.GEOMETRY_TYPE_NAME).primitiveCategory);
  }

  @Test
  public void testObjectInspectors() {
    GeometryTypeInfo crs3857 = TypeInfoFactory.getGeometryTypeInfo("EPSG:3857");
    PrimitiveObjectInspector writableOI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(crs3857);
    PrimitiveObjectInspector javaOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(crs3857);
    for (PrimitiveObjectInspector oi : List.of(writableOI, javaOI)) {
      Assert.assertTrue(oi instanceof BinaryObjectInspector);
      Assert.assertEquals(crs3857, oi.getTypeInfo());
      Assert.assertEquals("geometry('EPSG:3857')", oi.getTypeName());
    }
    Assert.assertSame(PrimitiveObjectInspectorFactory.writableGeometryObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.geometryTypeInfo));
    Assert.assertEquals(crs3857, ((PrimitiveObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(crs3857, new BytesWritable(WKB_POINT))).getTypeInfo());
    Assert.assertEquals(crs3857, TypeInfoUtils.getTypeInfoFromObjectInspector(
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(crs3857)));
  }

  @Test
  public void testValuesCompareAndHashLikeBinary() {
    ObjectInspector oi = PrimitiveObjectInspectorFactory.writableGeometryObjectInspector;
    BytesWritable a = new BytesWritable(WKB_POINT);
    BytesWritable b = new BytesWritable(WKB_POINT.clone());
    Assert.assertEquals(0, ObjectInspectorUtils.compare(a, oi, b, oi));
    Assert.assertEquals(ObjectInspectorUtils.hashCode(a, oi), ObjectInspectorUtils.hashCode(b, oi));
    Assert.assertTrue(PrimitiveObjectInspectorUtils.comparePrimitiveObjects(
        a, (PrimitiveObjectInspector) oi, b, (PrimitiveObjectInspector) oi));
  }

  @Test
  public void testConversions() {
    Assert.assertTrue(TypeInfoUtils.implicitConvertible(TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.geometryTypeInfo));
    Assert.assertTrue(TypeInfoUtils.implicitConvertible(TypeInfoFactory.voidTypeInfo,
        TypeInfoFactory.geometryTypeInfo));
    Assert.assertFalse(TypeInfoUtils.implicitConvertible(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.geometryTypeInfo));
    Assert.assertFalse(TypeInfoUtils.implicitConvertible(TypeInfoFactory.geometryTypeInfo,
        TypeInfoFactory.stringTypeInfo));
    Assert.assertTrue(TypeInfoUtils.implicitConvertible(TypeInfoFactory.getGeometryTypeInfo("EPSG:3857"),
        TypeInfoFactory.getGeometryTypeInfo("epsg:3857")));
    Assert.assertFalse(TypeInfoUtils.implicitConvertible(TypeInfoFactory.geometryTypeInfo,
        TypeInfoFactory.getGeometryTypeInfo("EPSG:3857")));
    Assert.assertFalse(TypeInfoUtils.implicitConvertible(TypeInfoFactory.getGeometryTypeInfo("EPSG:3857"),
        TypeInfoFactory.geometryTypeInfo));

    ObjectInspectorConverters.Converter converter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
        PrimitiveObjectInspectorFactory.writableGeometryObjectInspector);
    Assert.assertEquals(new BytesWritable(WKB_POINT), converter.convert(WKB_POINT));
    Assert.assertThrows(RuntimeException.class, () -> ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableGeometryObjectInspector));

    Assert.assertEquals(new BytesWritable(WKB_POINT), PrimitiveObjectInspectorUtils.getBinary(
        new BytesWritable(WKB_POINT), PrimitiveObjectInspectorFactory.writableGeometryObjectInspector));
  }

  @Test
  public void testLazyBinaryRoundTrip() throws SerDeException {
    GeometryTypeInfo crs3857 = TypeInfoFactory.getGeometryTypeInfo("EPSG:3857");
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "g");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, crs3857.getTypeName());
    LazyBinarySerDe serde = new LazyBinarySerDe();
    serde.initialize(new Configuration(), props, null);

    StandardStructObjectInspector rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        List.of("g"), List.of(
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(crs3857)));
    Writable serialized = serde.serialize(List.of(new BytesWritable(WKB_POINT)), rowOI);
    Object row = serde.deserialize(serialized);

    StructObjectInspector outOI = (StructObjectInspector) serde.getObjectInspector();
    BinaryObjectInspector fieldOI =
        (BinaryObjectInspector) outOI.getAllStructFieldRefs().get(0).getFieldObjectInspector();
    Assert.assertEquals(crs3857, fieldOI.getTypeInfo());
    Assert.assertArrayEquals(WKB_POINT,
        fieldOI.getPrimitiveJavaObject(outOI.getStructFieldData(row, outOI.getAllStructFieldRefs().get(0))));
  }

  @Test
  public void testThriftTypeMapping() {
    Assert.assertEquals(Type.GEOMETRY_TYPE, Type.getType(TypeInfoFactory.geometryTypeInfo));
    Assert.assertEquals(Type.GEOMETRY_TYPE, Type.getType("geometry('EPSG:3857')"));
    Assert.assertEquals(java.sql.Types.BINARY, Type.GEOMETRY_TYPE.toJavaSQLType());
  }
}
