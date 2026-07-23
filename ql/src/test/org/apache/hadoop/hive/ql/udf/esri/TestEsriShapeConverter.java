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
    Geometry back = EsriShapeConverter.fromEsriShapeBody(ByteBuffer.wrap(shape));
    assertNotNull("Round-trip of " + text + " must not be null!", back);
    return back;
  }

  /**
   * Asserts coordinate-level equality after canonicalizing both geometries with
   * {@link Geometry#normalize()}. Unlike {@code equalsTopo}, this catches ring reordering,
   * ring/hole misassignment and orientation bugs (normalize fixes orientation and vertex
   * order, so anything left is a real coordinate difference).
   */
  private static void assertEqualsNormalized(Geometry expected, Geometry actual) {
    Geometry e = expected.copy();
    Geometry a = actual.copy();
    e.normalize();
    a.normalize();
    assertTrue("Expected " + e + " but was " + a, e.equalsExact(a, EPSILON));
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
    assertEqualsNormalized(wkt(text), back);
  }

  @Test
  public void testPolygonWithHoleRoundTrip() throws Exception {
    String text = "polygon ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2))";
    Polygon back = (Polygon) roundTrip(text);
    assertEquals(1, back.getNumInteriorRing());
    assertEqualsNormalized(wkt(text), back);
  }

  @Test
  public void testMultiPolygonRoundTrip() throws Exception {
    String text = "multipolygon (((0 0, 4 0, 4 4, 0 4, 0 0)), ((10 10, 14 10, 14 14, 10 14, 10 10)))";
    Geometry back = roundTrip(text);
    assertTrue("Expected a MultiPolygon!", back instanceof MultiPolygon);
    assertEquals(2, back.getNumGeometries());
    assertEqualsNormalized(wkt(text), back);
  }

  /**
   * The most complex (de)serialization case: a MultiPolygon whose members carry holes.
   * Exercises ring-orientation handling and correct hole-to-shell assignment on read.
   */
  @Test
  public void testMultiPolygonWithHolesRoundTrip() throws Exception {
    String text = "multipolygon ("
        + "((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2)), "
        + "((20 20, 30 20, 30 30, 20 30, 20 20), (22 22, 22 24, 24 24, 24 22, 22 22)))";
    MultiPolygon back = (MultiPolygon) roundTrip(text);
    assertEquals(2, back.getNumGeometries());
    assertEquals(1, ((Polygon) back.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, ((Polygon) back.getGeometryN(1)).getNumInteriorRing());
    assertEqualsNormalized(wkt(text), back);
  }

  @Test
  public void testEmptyAndNullGeometryWriteNullShape() throws Exception {
    assertEquals(0, shapeType(EsriShapeConverter.toEsriShape(null)));
    assertEquals(0, shapeType(EsriShapeConverter.toEsriShape(wkt("point empty"))));
    assertNull(EsriShapeConverter.fromEsriShapeBody(ByteBuffer.wrap(EsriShapeConverter.toEsriShape(null))));
  }

  private static byte[] hex(String s) {
    byte[] out = new byte[s.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }

  /**
   * Golden bytes hand-encoded from the ESRI Shapefile spec (little-endian), so this checks
   * the reader against the on-disk format itself rather than only against our own writer.
   * Point shape: type(1) + X(1.0) + Y(2.0).
   */
  @Test
  public void testReadSpecPointBytes() {
    byte[] bytes = hex("01000000" + "000000000000F03F" + "0000000000000040");
    Point point = (Point) EsriShapeConverter.fromEsriShapeBody(ByteBuffer.wrap(bytes));
    assertEquals(1, point.getX(), EPSILON);
    assertEquals(2, point.getY(), EPSILON);
  }

  /**
   * Golden MultiPoint bytes from the ESRI spec, which carry a bounding box between the shape
   * type and the point count. Exercises the reader's bounding-box handling on input.
   * MultiPoint: type(8) + bbox(10,30,40,40) + numPoints(2) + (10,40),(40,30).
   */
  @Test
  public void testReadSpecMultiPointBytesWithBoundingBox() {
    byte[] bytes = hex("08000000"
        + "0000000000002440" + "0000000000003E40" + "0000000000004440" + "0000000000004440"  // bbox
        + "02000000"
        + "0000000000002440" + "0000000000004440"   // (10, 40)
        + "0000000000004440" + "0000000000003E40"); // (40, 30)
    MultiPoint mp = (MultiPoint) EsriShapeConverter.fromEsriShapeBody(ByteBuffer.wrap(bytes));
    assertEquals(2, mp.getNumGeometries());
    assertTrue(mp.equalsTopo(GeometryUtils.GEOMETRY_FACTORY.createMultiPoint(new Point[] {
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new org.locationtech.jts.geom.Coordinate(10, 40)),
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new org.locationtech.jts.geom.Coordinate(40, 30))})));
  }

  /**
   * The bounding box our writer emits (4 doubles right after the shape-type int) must match
   * the geometry's actual envelope.
   */
  @Test
  public void testWrittenBoundingBox() throws Exception {
    byte[] shape = EsriShapeConverter.toEsriShape(wkt("linestring (2 4, 10 10, 7 8)"));
    ByteBuffer buf = ByteBuffer.wrap(shape).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(3, buf.getInt(0));            // Polyline
    assertEquals(2.0, buf.getDouble(4), EPSILON);   // xmin
    assertEquals(4.0, buf.getDouble(12), EPSILON);  // ymin
    assertEquals(10.0, buf.getDouble(20), EPSILON); // xmax
    assertEquals(10.0, buf.getDouble(28), EPSILON); // ymax
  }

  /**
   * PointM (type 21): X(0) Y(3) M(1). M-bearing shapes are read for backward compatibility
   * with data serialized by the old ESRI library, even though we no longer write them.
   */
  @Test
  public void testReadSpecPointMBytes() {
    byte[] bytes = hex("15000000" + "0000000000000000" + "0000000000000840" + "000000000000F03F");
    Point point = (Point) EsriShapeConverter.fromEsriShapeBody(ByteBuffer.wrap(bytes));
    assertEquals(0, point.getX(), EPSILON);
    assertEquals(3, point.getY(), EPSILON);
    assertEquals(1, point.getCoordinate().getM(), EPSILON);
    assertTrue("Z must be absent", Double.isNaN(point.getCoordinate().getZ()));
  }

  /**
   * PointZ (type 11) carrying the optional trailing M value: X(0) Y(3) Z(1) M(2).
   * This is how a {@code POINT ZM} was stored by the old library.
   */
  @Test
  public void testReadSpecPointZMBytes() {
    byte[] bytes = hex("0B000000"
        + "0000000000000000" + "0000000000000840" + "000000000000F03F" + "0000000000000040");
    Point point = (Point) EsriShapeConverter.fromEsriShapeBody(ByteBuffer.wrap(bytes));
    assertEquals(1, point.getCoordinate().getZ(), EPSILON);
    assertEquals(2, point.getCoordinate().getM(), EPSILON);
  }

  /**
   * PolylineM (type 23), one part, points (10 10 m=2), (20 20 m=4): XY block then the
   * mandatory M block (Mmin, Mmax, per-vertex M).
   */
  @Test
  public void testReadSpecPolylineMBytes() {
    ByteBuffer buf = ByteBuffer.allocate(4 + 32 + 4 + 4 + 4 + 2 * 16 + 16 + 2 * 8)
        .order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(23);
    buf.putDouble(10).putDouble(10).putDouble(20).putDouble(20);   // bbox
    buf.putInt(1);                                                  // numParts
    buf.putInt(2);                                                  // numPoints
    buf.putInt(0);                                                  // part start
    buf.putDouble(10).putDouble(10).putDouble(20).putDouble(20);    // XY
    buf.putDouble(2).putDouble(4);                                  // Mmin, Mmax
    buf.putDouble(2).putDouble(4);                                  // per-vertex M
    buf.flip();

    LineString line = (LineString) EsriShapeConverter.fromEsriShapeBody(buf);
    assertEquals(2, line.getCoordinateN(0).getM(), EPSILON);
    assertEquals(4, line.getCoordinateN(1).getM(), EPSILON);
    assertTrue("Z must be absent", Double.isNaN(line.getCoordinateN(0).getZ()));
  }

  /** A PolylineZ with no trailing M block must not be reported as measured. */
  @Test
  public void testZShapeWithoutMeasureStaysUnmeasured() throws Exception {
    LineString back = (LineString) roundTrip("linestring z (0 0 1, 1 1 2)");
    assertTrue("M must be absent", Double.isNaN(back.getCoordinateN(0).getM()));
    assertEquals(1, back.getCoordinateN(0).getZ(), EPSILON);
  }

  @Test
  public void testUnsupportedShapeAndGeometryType() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(23);  // not a shape type this converter handles
    buffer.flip();
    assertThrows(IllegalArgumentException.class, () -> EsriShapeConverter.fromEsriShapeBody(buffer));

    Geometry collection = wkt("geometrycollection (point (1 2))");
    assertThrows(IllegalArgumentException.class, () -> EsriShapeConverter.toEsriShape(collection));
  }
}
