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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestEsriJsonConverter {

  private static final double EPSILON = 1e-9;

  private static final WKTReader WKT_READER = new WKTReader(GeometryUtils.GEOMETRY_FACTORY);

  private static Geometry wkt(String text) throws ParseException {
    return WKT_READER.read(text);
  }

  private static Geometry roundTrip(String text, int wkid) throws ParseException {
    Geometry geom = wkt(text);
    Geometry back = EsriJsonConverter.esriJsonToGeometry(EsriJsonConverter.geometryToEsriJson(geom, wkid));
    assertNotNull("Round-trip of " + text + " must not be null!", back);
    return back;
  }

  @Test
  public void testPointToJson() throws Exception {
    assertEquals("{\"x\":2,\"y\":3}", EsriJsonConverter.geometryToEsriJson(wkt("point (2 3)"), 0));
    assertEquals("{\"x\":2.5,\"y\":3.5,\"spatialReference\":{\"wkid\":4326}}",
        EsriJsonConverter.geometryToEsriJson(wkt("point (2.5 3.5)"), 4326));
    assertEquals("{\"x\":2,\"y\":3,\"z\":4}", EsriJsonConverter.geometryToEsriJson(wkt("point z (2 3 4)"), 0));
  }

  @Test
  public void testMultiPointToJson() throws Exception {
    assertEquals("{\"points\":[[1,2],[3,4]]}",
        EsriJsonConverter.geometryToEsriJson(wkt("multipoint ((1 2), (3 4))"), 0));
  }

  @Test
  public void testLineStringToJson() throws Exception {
    assertEquals("{\"paths\":[[[1,2],[3,4]]]}",
        EsriJsonConverter.geometryToEsriJson(wkt("linestring (1 2, 3 4)"), 0));
    assertEquals("{\"paths\":[[[1,2],[3,4]],[[5,6],[7,8]]]}",
        EsriJsonConverter.geometryToEsriJson(wkt("multilinestring ((1 2, 3 4), (5 6, 7 8))"), 0));
  }

  @Test
  public void testPolygonToJsonUsesEsriWindingOrder() throws Exception {
    // JTS exterior rings are CCW, Esri expects them clockwise, so the ring is reversed on output.
    assertEquals("{\"rings\":[[[0,0],[0,4],[4,4],[4,0],[0,0]]]}",
        EsriJsonConverter.geometryToEsriJson(wkt("polygon ((0 0, 4 0, 4 4, 0 4, 0 0))"), 0));
  }

  @Test
  public void testPointRoundTrip() throws Exception {
    Point point = (Point) roundTrip("point (12.224 51.829)", 4326);
    assertEquals(12.224, point.getX(), EPSILON);
    assertEquals(51.829, point.getY(), EPSILON);
    assertEquals(4326, point.getSRID());
  }

  @Test
  public void testPointZRoundTrip() throws Exception {
    Point point = (Point) roundTrip("point z (1 2 3)", 0);
    assertEquals(3, point.getCoordinate().getZ(), EPSILON);
  }

  @Test
  public void testMultiPointRoundTrip() throws Exception {
    Geometry back = roundTrip("multipoint ((1 2), (3 4), (5 6))", 0);
    assertTrue("Expected a MultiPoint!", back instanceof MultiPoint);
    assertTrue(back.equalsTopo(wkt("multipoint ((1 2), (3 4), (5 6))")));
  }

  @Test
  public void testLineStringRoundTrip() throws Exception {
    Geometry back = roundTrip("linestring (0 0, 1 1, 2 0)", 0);
    assertTrue("Expected a LineString!", back instanceof LineString);
    assertTrue(back.equalsTopo(wkt("linestring (0 0, 1 1, 2 0)")));
  }

  @Test
  public void testMultiLineStringRoundTrip() throws Exception {
    Geometry back = roundTrip("multilinestring ((0 0, 1 1), (2 2, 3 3))", 0);
    assertTrue("Expected a MultiLineString!", back instanceof MultiLineString);
    assertTrue(back.equalsTopo(wkt("multilinestring ((0 0, 1 1), (2 2, 3 3))")));
  }

  @Test
  public void testPolygonRoundTrip() throws Exception {
    Geometry back = roundTrip("polygon ((0 0, 4 0, 4 4, 0 4, 0 0))", 3857);
    assertTrue("Expected a Polygon!", back instanceof Polygon);
    assertTrue(back.equalsTopo(wkt("polygon ((0 0, 4 0, 4 4, 0 4, 0 0))")));
    assertEquals(3857, back.getSRID());
  }

  @Test
  public void testPolygonWithHoleRoundTrip() throws Exception {
    String text = "polygon ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2))";
    Polygon back = (Polygon) roundTrip(text, 0);
    assertEquals(1, back.getNumInteriorRing());
    assertTrue(back.equalsTopo(wkt(text)));
  }

  @Test
  public void testMultiPolygonRoundTrip() throws Exception {
    String text = "multipolygon (((0 0, 4 0, 4 4, 0 4, 0 0)), ((10 10, 14 10, 14 14, 10 14, 10 10)))";
    Geometry back = roundTrip(text, 0);
    assertTrue("Expected a MultiPolygon!", back instanceof MultiPolygon);
    assertEquals(2, back.getNumGeometries());
    assertTrue(back.equalsTopo(wkt(text)));
  }

  @Test
  public void testUnsupportedGeometryType() throws Exception {
    Geometry collection = wkt("geometrycollection (point (1 2))");
    assertThrows(IllegalArgumentException.class, () -> EsriJsonConverter.geometryToEsriJson(collection, 0));
    assertThrows(IllegalArgumentException.class, () -> EsriJsonConverter.geometryToEsriJson(null, 0));
  }

  @Test
  public void testUnparsableJson() {
    assertNull(EsriJsonConverter.esriJsonToGeometry(null));
    assertNull(EsriJsonConverter.esriJsonToGeometry(""));
    assertNull(EsriJsonConverter.esriJsonToGeometry("not json"));
    assertNull(EsriJsonConverter.esriJsonToGeometry("[1,2]"));
    assertNull(EsriJsonConverter.esriJsonToGeometry("{\"foo\":1}"));
    assertNull(EsriJsonConverter.esriJsonToGeometry("{\"paths\":[]}"));
    assertNull(EsriJsonConverter.esriJsonToGeometry("{\"rings\":[]}"));
  }
}
