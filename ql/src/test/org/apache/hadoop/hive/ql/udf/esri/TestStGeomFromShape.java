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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestStGeomFromShape {

  private final static double Epsilon = 0.0001;

  private static Point createFirstLocation() {
    final double longitude = 12.224;
    final double latitude = 51.829;
    return new Point(longitude, latitude);
  }

  private static Point createSecondLocation() {
    final double longitude = 12.39807;
    final double latitude = 51.34933;
    return new Point(longitude, latitude);
  }

  private static Point createThirdLocation() {
    final double longitude = 6.9823;
    final double latitude = 50.7657;
    return new Point(longitude, latitude);
  }

  private static Point createFourthLocation() {
    final double longitude = 7.102594;
    final double latitude = 50.73733;
    return new Point(longitude, latitude);
  }

  private static Polyline createLine() {
    Polyline line = new Polyline();
    line.startPath(createFirstLocation());
    line.lineTo(createSecondLocation());
    return line;
  }

  private static Polyline createPolyline() {
    Polyline line = new Polyline();
    line.startPath(createFirstLocation());
    line.lineTo(createSecondLocation());
    line.lineTo(createThirdLocation());
    line.lineTo(createFourthLocation());
    return line;
  }

  private static Polygon createPolygon() {
    Polygon polygon = new Polygon();
    polygon.startPath(createFirstLocation());
    polygon.lineTo(createSecondLocation());
    polygon.lineTo(createThirdLocation());
    polygon.lineTo(createFourthLocation());
    polygon.closeAllPaths();
    return polygon;
  }

  /**
   * Validates the geometry writable.
   *
   * @param point
   *            the represented point location.
   * @param wkid
   *            the represented spatial reference ID.
   * @param geometryAsWritable
   *            the geometry represented as {@link BytesWritable}.
   */
  private static void validatePoint(Point point, int wkid, BytesWritable geometryAsWritable) {
    ST_X getX = new ST_X();
    DoubleWritable xAsWritable = getX.evaluate(geometryAsWritable);
    assertNotNull("The x writable must not be null!", xAsWritable);

    ST_Y getY = new ST_Y();
    DoubleWritable yAsWritable = getY.evaluate(geometryAsWritable);
    assertNotNull("The y writable must not be null!", yAsWritable);

    assertEquals("Longitude is different!", point.getX(), xAsWritable.get(), Epsilon);
    assertEquals("Latitude is different!", point.getY(), yAsWritable.get(), Epsilon);

    ST_SRID getWkid = new ST_SRID();
    IntWritable wkidAsWritable = getWkid.evaluate(geometryAsWritable);
    assertNotNull("The wkid writable must not be null!", wkidAsWritable);

    assertEquals("The wkid is different!", wkid, wkidAsWritable.get());
  }

  @Test
  public void testGeomFromPointShapeWithoutSpatialReference() throws UDFArgumentException {
    Point point = createFirstLocation();

    byte[] esriShape = GeometryEngine.geometryToEsriShape(point);
    assertNotNull("The shape must not be null!", esriShape);

    BytesWritable shapeAsWritable = new BytesWritable(esriShape);
    assertNotNull("The shape writable must not be null!", shapeAsWritable);

    ST_GeomFromShape fromShape = new ST_GeomFromShape();
    BytesWritable geometryAsWritable = fromShape.evaluate(shapeAsWritable);
    assertNotNull("The geometry writable must not be null!", geometryAsWritable);

    final int wkid = 0;
    validatePoint(point, wkid, geometryAsWritable);
  }

  @Test
  public void testGeomFromPointShape() throws UDFArgumentException {
    Point point = createFirstLocation();
    byte[] esriShape = GeometryEngine.geometryToEsriShape(point);
    assertNotNull("The shape must not be null!", esriShape);

    BytesWritable shapeAsWritable = new BytesWritable(esriShape);
    assertNotNull("The shape writable must not be null!", shapeAsWritable);

    final int wkid = 4326;
    ST_GeomFromShape fromShape = new ST_GeomFromShape();
    BytesWritable geometryAsWritable = fromShape.evaluate(shapeAsWritable, wkid);
    assertNotNull("The geometry writable must not be null!", geometryAsWritable);

    validatePoint(point, wkid, geometryAsWritable);
  }

  @Test
  public void testGeomFromLineShape() throws UDFArgumentException {
    Polyline line = createLine();
    byte[] esriShape = GeometryEngine.geometryToEsriShape(line);
    assertNotNull("The shape must not be null!", esriShape);

    BytesWritable shapeAsWritable = new BytesWritable(esriShape);
    assertNotNull("The shape writable must not be null!", shapeAsWritable);

    final int wkid = 4326;
    ST_GeomFromShape fromShape = new ST_GeomFromShape();
    BytesWritable geometryAsWritable = fromShape.evaluate(shapeAsWritable, wkid);
    assertNotNull("The geometry writable must not be null!", geometryAsWritable);

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geometryAsWritable);
    assertNotNull("The OGC geometry must not be null!", ogcGeometry);

    Geometry esriGeometry = ogcGeometry.getEsriGeometry();
    assertNotNull("The Esri geometry must not be null!", esriGeometry);
    assertTrue("The geometries are different!",
        GeometryEngine.equals(line, esriGeometry, SpatialReference.create(wkid)));
  }

  @Test
  public void testGeomFromPolylineShape() throws UDFArgumentException {
    Polyline line = createPolyline();
    byte[] esriShape = GeometryEngine.geometryToEsriShape(line);
    assertNotNull("The shape must not be null!", esriShape);

    BytesWritable shapeAsWritable = new BytesWritable(esriShape);
    assertNotNull("The shape writable must not be null!", shapeAsWritable);

    final int wkid = 4326;
    ST_GeomFromShape fromShape = new ST_GeomFromShape();
    BytesWritable geometryAsWritable = fromShape.evaluate(shapeAsWritable, wkid);
    assertNotNull("The geometry writable must not be null!", geometryAsWritable);

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geometryAsWritable);
    assertNotNull("The OGC geometry must not be null!", ogcGeometry);

    Geometry esriGeometry = ogcGeometry.getEsriGeometry();
    assertNotNull("The Esri geometry must not be null!", esriGeometry);
    assertTrue("The geometries are different!",
        GeometryEngine.equals(line, esriGeometry, SpatialReference.create(wkid)));
  }

  @Test
  public void testGeomFromPolygonShape() throws UDFArgumentException {
    Polygon polygon = createPolygon();
    byte[] esriShape = GeometryEngine.geometryToEsriShape(polygon);
    assertNotNull("The shape must not be null!", esriShape);

    BytesWritable shapeAsWritable = new BytesWritable(esriShape);
    assertNotNull("The shape writable must not be null!", shapeAsWritable);

    final int wkid = 4326;
    ST_GeomFromShape fromShape = new ST_GeomFromShape();
    BytesWritable geometryAsWritable = fromShape.evaluate(shapeAsWritable, wkid);
    assertNotNull("The geometry writable must not be null!", geometryAsWritable);

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geometryAsWritable);
    assertNotNull("The OGC geometry must not be null!", ogcGeometry);

    Geometry esriGeometry = ogcGeometry.getEsriGeometry();
    assertNotNull("The Esri geometry must not be null!", esriGeometry);
    assertTrue("The geometries are different!",
        GeometryEngine.equals(polygon, esriGeometry, SpatialReference.create(wkid)));
  }
}
