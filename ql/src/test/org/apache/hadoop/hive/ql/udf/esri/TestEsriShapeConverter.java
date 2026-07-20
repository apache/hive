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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.esri;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestEsriShapeConverter {

  private static final double EPSILON = 1e-9;

  private static final WKTReader WKT_READER = new WKTReader(GeometryUtils.GEOMETRY_FACTORY);

  private static Geometry wkt(String text) throws ParseException {
    return WKT_READER.read(text);
  }

  private static Geometry roundTrip(String text) throws ParseException {
    Geometry geom = wkt(text);
    byte[] shape = EsriShapeConverter.toEsriShape(geom);
    Geometry back = EsriShapeConverter.fromEsriShape(ByteBuffer.wrap(shape));
    assertNotNull("Round-trip of " + text + " must not be null!", back);
    return back;
  }

  private static int shapeType(byte[] shape) {
    return ByteBuffer.wrap(shape).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  @Test
  public void testPointRoundTrip() throws Exception {
    Point point = (Point) roundTrip("point (12.224 51.829)");
    assertEquals(12.224, point.getX(), EPSILON);
    assertEquals(51.829, point.getY(), EPSILON);
  }

  @Test
  public void testPointZRoundTrip() throws Exception {
    Point point = (Point) roundTrip("point z (1 2 3)");
    assertEquals(1, point.getX(), EPSILON);
    assertEquals(2, point.getY(), EPSILON);
    assertEquals(3, point.getCoordinate().getZ(), EPSILON);
    assertEquals(11, shapeType(EsriShapeConverter.toEsriShape(wkt("point z (1 2 3)"))));
  }

  @Test
  public void testMultiPointRoundTrip() throws Exception {
    Geometry back = roundTrip("multipoint ((1 2), (3 4), (5 6))");
    assertTrue("Expected a MultiPoint!", back instanceof MultiPoint);
    assertTrue(back.equalsTopo(wkt("multipoint ((1 2), (3 4), (5 6))")));
  }

  @Test
  public void testMultiPointZRoundTrip() throws Exception {
    MultiPoint back = (MultiPoint) roundTrip("multipoint z ((1 2 3), (4 5 6))");
    assertEquals(3, back.getGeometryN(0).getCoordinate().getZ(), EPSILON);
    assertEquals(6, back.getGeometryN(1).getCoordinate().getZ(), EPSILON);
  }

  @Test
  public void testLineStringRoundTrip() throws Exception {
    Geometry back = roundTrip("linestring (0 0, 1 1, 2 0)");
    assertTrue("Expected a LineString!", back instanceof LineString);
    assertTrue(back.equalsTopo(wkt("linestring (0 0, 1 1, 2 0)")));
  }

  @Test
  public void testMultiLineStringRoundTrip() throws Exception {
    String text = "multilinestring ((0 0, 1 1), (2 2, 3 3))";
    Geometry back = roundTrip(text);
    assertTrue("Expected a MultiLineString!", back instanceof MultiLineString);
    assertTrue(back.equalsTopo(wkt(text)));
  }

  @Test
  public void testLineStringZRoundTrip() throws Exception {
    LineString back = (LineString) roundTrip("linestring z (0 0 1, 1 1 2)");
    assertEquals(1, back.getCoordinateN(0).getZ(), EPSILON);
    assertEquals(2, back.getCoordinateN(1).getZ(), EPSILON);
  }

  @Test
  public void testPolygonRoundTrip() throws Exception {
    String text = "polygon ((0 0, 4 0, 4 4, 0 4, 0 0))";
    Geometry back = roundTrip(text);
    assertTrue("Expected a Polygon!", back instanceof Polygon);
    assertTrue(back.equalsTopo(wkt(text)));
  }

  @Test
  public void testPolygonWithHoleRoundTrip() throws Exception {
    String text = "polygon ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2))";
    Polygon back = (Polygon) roundTrip(text);
    assertEquals(1, back.getNumInteriorRing());
    assertTrue(back.equalsTopo(wkt(text)));
  }

  @Test
  public void testMultiPolygonRoundTrip() throws Exception {
    String text = "multipolygon (((0 0, 4 0, 4 4, 0 4, 0 0)), ((10 10, 14 10, 14 14, 10 14, 10 10)))";
    Geometry back = roundTrip(text);
    assertTrue("Expected a MultiPolygon!", back instanceof MultiPolygon);
    assertEquals(2, back.getNumGeometries());
    assertTrue(back.equalsTopo(wkt(text)));
  }

  @Test
  public void testEmptyAndNullGeometryWriteNullShape() throws Exception {
    assertEquals(0, shapeType(EsriShapeConverter.toEsriShape(null)));
    assertEquals(0, shapeType(EsriShapeConverter.toEsriShape(wkt("point empty"))));
    assertNull(EsriShapeConverter.fromEsriShape(ByteBuffer.wrap(EsriShapeConverter.toEsriShape(null))));
  }

  @Test
  public void testUnsupportedShapeAndGeometryType() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(23);  // not a shape type this converter handles
    buffer.flip();
    assertThrows(IllegalArgumentException.class, () -> EsriShapeConverter.fromEsriShape(buffer));

    Geometry collection = wkt("geometrycollection (point (1 2))");
    assertThrows(IllegalArgumentException.class, () -> EsriShapeConverter.toEsriShape(collection));
  }
}
