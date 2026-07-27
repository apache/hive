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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Converts between JTS geometries and Esri JSON format without any dependency
 * on the ESRI geometry library.
 *
 * <p>Esri JSON format reference:
 * <ul>
 *   <li>Point:            {@code {"x":N,"y":N,"spatialReference":{"wkid":N}}}</li>
 *   <li>MultiPoint:       {@code {"points":[[x,y],...],"spatialReference":{"wkid":N}}}</li>
 *   <li>LineString:       {@code {"paths":[[[x,y],...]],"spatialReference":{"wkid":N}}}</li>
 *   <li>MultiLineString:  {@code {"paths":[[[x,y],...],...],"spatialReference":{"wkid":N}}}</li>
 *   <li>Polygon:          {@code {"rings":[[[x,y],...,[x,y]]],"spatialReference":{"wkid":N}}}</li>
 *   <li>MultiPolygon:     {@code {"rings":[[[x,y],...],...],"spatialReference":{"wkid":N}}}</li>
 * </ul>
 *
 * <p>Winding-order convention: Esri requires exterior rings to be clockwise (CW) and
 * interior rings (holes) to be counter-clockwise (CCW) when viewed in screen coordinates
 * (y-axis pointing down). JTS follows the OGC convention, which is the opposite.
 * This converter checks and reverses rings as necessary using
 * {@link Orientation#isCCW(Coordinate[])}.
 */
public final class EsriJsonConverter {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private EsriJsonConverter() {
  }

  /**
   * Converts a JTS {@link Geometry} to its Esri JSON string representation.
   *
   * @param geom the JTS geometry to convert; must not be {@code null}
   * @param wkid the Well-Known ID of the spatial reference (0 = omit spatialReference)
   * @return Esri JSON string
   * @throws IllegalArgumentException if the geometry type is not supported
   */
  public static String geometryToEsriJson(Geometry geom, int wkid) {
    if (geom == null) {
      throw new IllegalArgumentException("geom must not be null");
    }

    StringBuilder sb = new StringBuilder();

    if (geom instanceof Point point) {
      writePoint(point, wkid, sb);
    } else if (geom instanceof MultiPoint multiPoint) {
      writeMultiPoint(multiPoint, wkid, sb);
    } else if (geom instanceof LineString) {
      // Single path wrapped in the "paths" array
      sb.append("{\"paths\":[");
      writeCoordArray(geom.getCoordinates(), sb);
      sb.append(']');
      appendSpatialReference(wkid, sb);
      sb.append('}');
    } else if (geom instanceof MultiLineString multiLineString) {
      writeMultiLineString(multiLineString, wkid, sb);
    } else if (geom instanceof Polygon polygon) {
      sb.append("{\"rings\":[");
      writePolygonRings(polygon, sb);
      sb.append(']');
      appendSpatialReference(wkid, sb);
      sb.append('}');
    } else if (geom instanceof MultiPolygon multiPolygon) {
      writeMultiPolygon(multiPolygon, wkid, sb);
    } else {
      throw new IllegalArgumentException("Unsupported geometry type: " + geom.getGeometryType());
    }

    return sb.toString();
  }

  private static void writePoint(Point pt, int wkid, StringBuilder sb) {
    sb.append("{\"x\":");
    appendDouble(pt.getX(), sb);
    sb.append(",\"y\":");
    appendDouble(pt.getY(), sb);
    double z = pt.getCoordinate().getZ();
    if (!Double.isNaN(z)) {
      sb.append(",\"z\":");
      appendDouble(z, sb);
    }
    appendSpatialReference(wkid, sb);
    sb.append('}');
  }

  private static void writeMultiPoint(MultiPoint mp, int wkid, StringBuilder sb) {
    sb.append("{\"points\":[");
    int n = mp.getNumGeometries();
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      writeCoord(mp.getGeometryN(i).getCoordinate(), sb);
    }
    sb.append(']');
    appendSpatialReference(wkid, sb);
    sb.append('}');
  }

  private static void writeMultiLineString(MultiLineString mls, int wkid, StringBuilder sb) {
    sb.append("{\"paths\":[");
    int n = mls.getNumGeometries();
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      writeCoordArray(mls.getGeometryN(i).getCoordinates(), sb);
    }
    sb.append(']');
    appendSpatialReference(wkid, sb);
    sb.append('}');
  }

  /**
   * Writes the rings for a single {@link Polygon} into {@code sb} (without the outer
   * braces or the spatialReference). The caller is responsible for opening/closing
   * the "rings" key.
   *
   * <p>Esri winding order: exterior ring CW, interior rings CCW.
   */
  private static void writePolygonRings(Polygon poly, StringBuilder sb) {
    // Exterior ring: must be CW in Esri convention.
    LineString exterior = poly.getExteriorRing();
    Coordinate[] extCoords = ensureCW(exterior.getCoordinates());
    writeCoordArray(extCoords, sb);

    // Interior rings (holes): must be CCW in Esri convention.
    for (int h = 0; h < poly.getNumInteriorRing(); h++) {
      sb.append(',');
      Coordinate[] holeCoords = ensureCCW(poly.getInteriorRingN(h).getCoordinates());
      writeCoordArray(holeCoords, sb);
    }
  }

  private static void writeMultiPolygon(MultiPolygon mp, int wkid, StringBuilder sb) {
    sb.append("{\"rings\":[");
    boolean firstRing = true;
    int n = mp.getNumGeometries();
    for (int i = 0; i < n; i++) {
      Polygon poly = (Polygon) mp.getGeometryN(i);

      if (!firstRing) {
        sb.append(',');
      }
      firstRing = false;
      // Exterior ring
      Coordinate[] extCoords = ensureCW(poly.getExteriorRing().getCoordinates());
      writeCoordArray(extCoords, sb);

      // Interior rings
      for (int h = 0; h < poly.getNumInteriorRing(); h++) {
        sb.append(',');
        Coordinate[] holeCoords = ensureCCW(poly.getInteriorRingN(h).getCoordinates());
        writeCoordArray(holeCoords, sb);
      }
    }
    sb.append(']');
    appendSpatialReference(wkid, sb);
    sb.append('}');
  }

  /** Writes a coordinate sequence as a JSON array: {@code [[x,y],[x,y],...]}. */
  private static void writeCoordArray(Coordinate[] coords, StringBuilder sb) {
    sb.append('[');
    for (int i = 0; i < coords.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      writeCoord(coords[i], sb);
    }
    sb.append(']');
  }

  /** Writes a single coordinate as {@code [x,y]} or {@code [x,y,z]} if Z is present. */
  private static void writeCoord(Coordinate c, StringBuilder sb) {
    sb.append('[');
    appendDouble(c.x, sb);
    sb.append(',');
    appendDouble(c.y, sb);
    if (!Double.isNaN(c.getZ())) {
      sb.append(',');
      appendDouble(c.getZ(), sb);
    }
    sb.append(']');
  }

  /** Appends {@code ,"spatialReference":{"wkid":N}} unless {@code wkid == 0}. */
  private static void appendSpatialReference(int wkid, StringBuilder sb) {
    if (wkid != 0) {
      sb.append(",\"spatialReference\":{\"wkid\":");
      sb.append(wkid);
      sb.append('}');
    }
  }

  /**
   * Appends a double value, formatting whole numbers without a decimal point
   * (e.g. 2 not 2.0) to match Esri JSON conventions.
   */
  private static void appendDouble(double v, StringBuilder sb) {
    long lv = (long) v;
    if (v == lv) {
      sb.append(lv);
    } else {
      sb.append(Double.toString(v));
    }
  }

  /**
   * Returns the coordinate array reversed in-place if it is currently CCW
   * (i.e. makes it CW for Esri exterior rings).
   */
  private static Coordinate[] ensureCW(Coordinate[] coords) {
    if (Orientation.isCCW(coords)) {
      return reverse(coords);
    }
    return coords;
  }

  /**
   * Returns the coordinate array reversed if it is currently CW
   * (i.e. makes it CCW for Esri interior/hole rings).
   */
  private static Coordinate[] ensureCCW(Coordinate[] coords) {
    if (!Orientation.isCCW(coords)) {
      return reverse(coords);
    }
    return coords;
  }

  /** Returns a new array that is the reverse of {@code coords}. */
  private static Coordinate[] reverse(Coordinate[] coords) {
    Coordinate[] copy = Arrays.copyOf(coords, coords.length);
    for (int i = 0, j = copy.length - 1; i < j; i++, j--) {
      Coordinate tmp = copy[i];
      copy[i] = copy[j];
      copy[j] = tmp;
    }
    return copy;
  }

  /**
   * Parses an Esri JSON string into a JTS {@link Geometry}.
   *
   * <p>Supported Esri JSON geometry types:
   * <ul>
   *   <li>{@code {"x":N,"y":N}} → {@link Point}</li>
   *   <li>{@code {"points":[[x,y],...]}} → {@link MultiPoint}</li>
   *   <li>{@code {"paths":[[[x,y],...]...]}} → {@link LineString} (single path) or
   *       {@link MultiLineString} (multiple paths)</li>
   *   <li>{@code {"rings":[[[x,y],...]...]}} → {@link Polygon} or {@link MultiPolygon}</li>
   * </ul>
   *
   * <p>If a {@code "spatialReference":{"wkid":N}} object is present the SRID is set on
   * the returned geometry.
   *
   * @param json the Esri JSON string; must not be {@code null}
   * @return the parsed JTS geometry, or {@code null} if the string cannot be parsed
   */
  public static Geometry esriJsonToGeometry(String json) {
    if (json == null || json.isEmpty()) {
      return null;
    }

    JsonNode root;
    try {
      root = OBJECT_MAPPER.readTree(json);
    } catch (IOException e) {
      return null;
    }

    if (!root.isObject()) {
      return null;
    }

    int wkid = parseSrid(root);
    Geometry geom;

    if (root.has("x")) {
      geom = parsePoint(root);
    } else if (root.has("points")) {
      geom = parseMultiPoint(root);
    } else if (root.has("paths")) {
      geom = parsePaths(root);
    } else if (root.has("rings")) {
      geom = parseRings(root);
    } else {
      return null;
    }

    if (geom != null && wkid != 0) {
      geom.setSRID(wkid);
    }
    return geom;
  }

  private static int parseSrid(JsonNode root) {
    JsonNode sr = root.get("spatialReference");
    if (sr != null && sr.has("wkid")) {
      return sr.get("wkid").asInt(0);
    }
    return 0;
  }

  private static Point parsePoint(JsonNode root) {
    double x = root.get("x").asDouble();
    double y = root.get("y").asDouble();
    Coordinate coord;
    if (root.has("z")) {
      coord = new Coordinate(x, y, root.get("z").asDouble());
    } else {
      coord = new Coordinate(x, y);
    }
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(coord);
  }

  private static MultiPoint parseMultiPoint(JsonNode root) {
    JsonNode pts = root.get("points");
    if (!pts.isArray()) {
      return null;
    }
    Coordinate[] coords = readCoordArray(pts);
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPointFromCoords(coords);
  }

  private static Geometry parsePaths(JsonNode root) {
    JsonNode paths = root.get("paths");
    if (!paths.isArray() || paths.isEmpty()) {
      return null;
    }
    LineString[] lines = new LineString[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      Coordinate[] coords = readCoordArray(paths.get(i));
      lines[i] = GeometryUtils.GEOMETRY_FACTORY.createLineString(coords);
    }
    if (lines.length == 1) {
      return lines[0];
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(lines);
  }

  /**
   * Parses an Esri JSON "rings" array into a {@link Polygon} or {@link MultiPolygon}.
   *
   * <p>Strategy: iterate rings; a ring is an exterior ring if it is CW (positive signed
   * area using the Shoelace formula in screen-coordinate convention, which is what Esri
   * uses). Each exterior ring starts a new polygon; CCW rings that follow are holes of
   * the preceding exterior ring.
   *
   * <p>In practice we use {@link Orientation#isCCW} to classify: a ring that is <em>not</em>
   * CCW (i.e. is CW) is treated as an exterior ring.
   */
  private static Geometry parseRings(JsonNode root) {
    JsonNode rings = root.get("rings");
    if (!rings.isArray() || rings.isEmpty()) {
      return null;
    }

    // Build list of (coords, isExterior) pairs
    List<Coordinate[]> exteriors = new ArrayList<>();
    // holes[i] is the list of hole coordinate arrays for exteriors.get(i)
    List<List<Coordinate[]>> holes = new ArrayList<>();

    for (int i = 0; i < rings.size(); i++) {
      Coordinate[] coords = readCoordArray(rings.get(i));
      // Ensure the ring is closed
      coords = ensureClosed(coords);

      // In Esri JSON: exterior rings are CW, interior (holes) are CCW.
      // Orientation.isCCW returns true for CCW rings.
      boolean isCCW = Orientation.isCCW(coords);
      if (!isCCW) {
        // CW → exterior ring
        exteriors.add(coords);
        holes.add(new ArrayList<>());
      } else {
        // CCW → hole belonging to the most recent exterior ring
        if (exteriors.isEmpty()) {
          // Defensive: treat as exterior if no preceding exterior ring found
          exteriors.add(coords);
          holes.add(new ArrayList<>());
        } else {
          holes.get(holes.size() - 1).add(coords);
        }
      }
    }

    if (exteriors.isEmpty()) {
      return null;
    }

    Polygon[] polygons = new Polygon[exteriors.size()];
    for (int i = 0; i < exteriors.size(); i++) {
      LinearRing shell = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(exteriors.get(i));
      List<Coordinate[]> holeList = holes.get(i);
      LinearRing[] holeRings = new LinearRing[holeList.size()];
      for (int h = 0; h < holeList.size(); h++) {
        holeRings[h] = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(holeList.get(h));
      }
      polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon(shell, holeRings);
    }

    if (polygons.length == 1) {
      return polygons[0];
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polygons);
  }

  /**
   * Reads a JSON array-of-arrays into a {@link Coordinate} array.
   * Each element must be {@code [x, y]} or {@code [x, y, z]}.
   */
  private static Coordinate[] readCoordArray(JsonNode array) {
    Coordinate[] coords = new Coordinate[array.size()];
    for (int i = 0; i < array.size(); i++) {
      JsonNode pt = array.get(i);
      double x = pt.get(0).asDouble();
      double y = pt.get(1).asDouble();
      if (pt.size() >= 3) {
        coords[i] = new Coordinate(x, y, pt.get(2).asDouble());
      } else {
        coords[i] = new Coordinate(x, y);
      }
    }
    return coords;
  }

  /**
   * Returns a coordinate array that is guaranteed to be closed (first == last).
   * If the input is already closed it is returned unchanged; otherwise a new array
   * with the first coordinate appended at the end is returned.
   */
  private static Coordinate[] ensureClosed(Coordinate[] coords) {
    if (coords.length == 0) {
      return coords;
    }
    Coordinate first = coords[0];
    Coordinate last = coords[coords.length - 1];
    if (first.equals3D(last)) {
      return coords;
    }
    Coordinate[] closed = Arrays.copyOf(coords, coords.length + 1);
    closed[coords.length] = first.copy();
    return closed;
  }
}
