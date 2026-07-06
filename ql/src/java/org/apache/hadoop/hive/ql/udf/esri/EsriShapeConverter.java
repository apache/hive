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

import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes raw ESRI shape binary format directly from/to JTS geometries,
 * without any dependency on the ESRI Geometry library.
 *
 * <p>All binary values are little-endian as per the ESRI shape format specification.
 *
 * <p>Supported shape types:
 * <ul>
 *   <li>0  - Null/Unknown</li>
 *   <li>1  - Point</li>
 *   <li>3  - Polyline (paths)</li>
 *   <li>5  - Polygon (rings)</li>
 *   <li>8  - MultiPoint</li>
 *   <li>11 - PointZ</li>
 *   <li>13 - PolylineZ</li>
 *   <li>15 - PolygonZ</li>
 *   <li>18 - MultiPointZ</li>
 * </ul>
 */
public final class EsriShapeConverter {

  private static final int TYPE_NULL       = 0;
  private static final int TYPE_POINT      = 1;
  private static final int TYPE_POLYLINE   = 3;
  private static final int TYPE_POLYGON    = 5;
  private static final int TYPE_MULTIPOINT = 8;
  private static final int TYPE_POINT_Z      = 11;
  private static final int TYPE_POLYLINE_Z   = 13;
  private static final int TYPE_POLYGON_Z    = 15;
  private static final int TYPE_MULTIPOINT_Z = 18;

  private EsriShapeConverter() {
  }

  /**
   * Reads the ESRI shape binary from {@code shapeBuffer} (already positioned at start,
   * little-endian byte order will be set internally) and returns a JTS {@link Geometry}.
   *
   * @param shapeBuffer buffer containing raw ESRI shape bytes starting at its current position
   * @return a JTS Geometry, or {@code null} for the Null/Unknown shape type
   * @throws IllegalArgumentException if the buffer contains an unsupported or malformed shape
   */
  public static Geometry fromEsriShape(ByteBuffer shapeBuffer) {
    shapeBuffer.order(ByteOrder.LITTLE_ENDIAN);
    int shapeType = shapeBuffer.getInt();

    return switch (shapeType) {
      case TYPE_NULL -> null;
      case TYPE_POINT -> readPoint(shapeBuffer, false);
      case TYPE_POINT_Z -> readPoint(shapeBuffer, true);
      case TYPE_MULTIPOINT -> readMultiPoint(shapeBuffer, false);
      case TYPE_MULTIPOINT_Z -> readMultiPoint(shapeBuffer, true);
      case TYPE_POLYLINE -> readPolyline(shapeBuffer, false);
      case TYPE_POLYLINE_Z -> readPolyline(shapeBuffer, true);
      case TYPE_POLYGON -> readPolygon(shapeBuffer, false);
      case TYPE_POLYGON_Z -> readPolygon(shapeBuffer, true);
      default -> throw new IllegalArgumentException("Unsupported ESRI shape type: " + shapeType);
    };
  }

  private static Point readPoint(ByteBuffer buf, boolean hasZ) {
    double x = buf.getDouble();
    double y = buf.getDouble();
    if (hasZ) {
      double z = buf.getDouble();
      return GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y, z));
    }
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
  }

  private static MultiPoint readMultiPoint(ByteBuffer buf, boolean hasZ) {
    // skip bounding box (4 doubles = 32 bytes)
    buf.position(buf.position() + 32);
    int numPoints = buf.getInt();
    double[] xs = new double[numPoints];
    double[] ys = new double[numPoints];
    for (int i = 0; i < numPoints; i++) {
      xs[i] = buf.getDouble();
      ys[i] = buf.getDouble();
    }
    double[] zs = null;
    if (hasZ) {
      // skip zMin, zMax
      buf.position(buf.position() + 16);
      zs = new double[numPoints];
      for (int i = 0; i < numPoints; i++) {
        zs[i] = buf.getDouble();
      }
    }
    Point[] points = new Point[numPoints];
    for (int i = 0; i < numPoints; i++) {
      Coordinate coord = (zs != null) ? new Coordinate(xs[i], ys[i], zs[i])
                                      : new Coordinate(xs[i], ys[i]);
      points[i] = GeometryUtils.GEOMETRY_FACTORY.createPoint(coord);
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPoint(points);
  }

  /**
   * Read a Polyline or PolylineZ. Buffer is positioned just after the type int.
   * Multiple parts become a MultiLineString; a single part becomes a LineString.
   */
  private static Geometry readPolyline(ByteBuffer buf, boolean hasZ) {
    // skip bounding box
    buf.position(buf.position() + 32);
    int numParts  = buf.getInt();
    int numPoints = buf.getInt();

    int[] partStarts = new int[numParts];
    for (int i = 0; i < numParts; i++) {
      partStarts[i] = buf.getInt();
    }

    Coordinate[] allCoords = readXYCoords(buf, numPoints);
    if (hasZ) {
      readZIntoCoords(buf, allCoords);
    }

    LineString[] lines = new LineString[numParts];
    for (int i = 0; i < numParts; i++) {
      int start = partStarts[i];
      int end   = (i + 1 < numParts) ? partStarts[i + 1] : numPoints;
      Coordinate[] partCoords = copyRange(allCoords, start, end);
      lines[i] = GeometryUtils.GEOMETRY_FACTORY.createLineString(partCoords);
    }

    if (lines.length == 1) {
      return lines[0];
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(lines);
  }

  /**
   * Read a Polygon or PolygonZ. Buffer is positioned just after the type int.
   *
   * <p>In ESRI shape format, exterior rings are wound clockwise (negative signed area)
   * and holes are wound counterclockwise (positive signed area). Multiple exterior rings
   * produce a MultiPolygon.
   */
  private static Geometry readPolygon(ByteBuffer buf, boolean hasZ) {
    // skip bounding box
    buf.position(buf.position() + 32);
    int numParts  = buf.getInt();
    int numPoints = buf.getInt();

    int[] partStarts = new int[numParts];
    for (int i = 0; i < numParts; i++) {
      partStarts[i] = buf.getInt();
    }

    Coordinate[] allCoords = readXYCoords(buf, numPoints);
    if (hasZ) {
      readZIntoCoords(buf, allCoords);
    }

    // Split into per-ring coordinate arrays
    Coordinate[][] rings = new Coordinate[numParts][];
    for (int i = 0; i < numParts; i++) {
      int start = partStarts[i];
      int end   = (i + 1 < numParts) ? partStarts[i + 1] : numPoints;
      rings[i] = copyRange(allCoords, start, end);
    }

    // Classify rings: clockwise = exterior (ESRI convention); CCW = hole
    // Build list of (exterior ring index, list of hole ring indices)
    List<Integer> exteriorIndices = new ArrayList<>();
    List<List<Integer>> holesByExterior = new ArrayList<>();

    for (int i = 0; i < rings.length; i++) {
      if (!Orientation.isCCW(rings[i])) {
        // clockwise -> exterior
        exteriorIndices.add(i);
        holesByExterior.add(new ArrayList<>());
      }
    }

    // Assign holes to the nearest preceding exterior ring
    int exteriorCount = exteriorIndices.size();
    for (int i = 0; i < rings.length; i++) {
      if (Orientation.isCCW(rings[i])) {
        // counterclockwise -> hole; assign to the last exterior ring that precedes it
        int ownerSlot = exteriorCount - 1;
        for (int e = 0; e < exteriorCount; e++) {
          if (exteriorIndices.get(e) > i) {
            ownerSlot = e - 1;
            break;
          }
        }
        if (ownerSlot < 0) {
          ownerSlot = 0;
        }
        holesByExterior.get(ownerSlot).add(i);
      }
    }

    // Build JTS Polygons
    Polygon[] polygons = new Polygon[exteriorCount];
    for (int e = 0; e < exteriorCount; e++) {
      LinearRing shell = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(rings[exteriorIndices.get(e)]);
      List<Integer> holeIndices = holesByExterior.get(e);
      LinearRing[] holes = new LinearRing[holeIndices.size()];
      for (int h = 0; h < holes.length; h++) {
        holes[h] = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(rings[holeIndices.get(h)]);
      }
      polygons[e] = GeometryUtils.GEOMETRY_FACTORY.createPolygon(shell, holes);
    }

    if (polygons.length == 1) {
      return polygons[0];
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polygons);
  }

  /**
   * Writes the JTS {@link Geometry} to ESRI shape binary format and returns the bytes.
   * All values are little-endian.
   *
   * @param geom the JTS geometry to serialize; if {@code null} a 4-byte null shape is returned
   * @return raw ESRI shape bytes
   * @throws IllegalArgumentException if the geometry type is not supported
   */
  public static byte[] toEsriShape(Geometry geom) {
    if (geom == null || geom.isEmpty()) {
      ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      buf.putInt(TYPE_NULL);
      return buf.array();
    }

    return switch (geom.getGeometryType()) {
      case "Point" -> writePoint((Point) geom);
      case "MultiPoint" -> writeMultiPoint((MultiPoint) geom);
      case "LineString", "LinearRing" -> writeLineString((LineString) geom);
      case "MultiLineString" -> writeMultiLineString((MultiLineString) geom);
      case "Polygon" -> writePolygonBuffer(collectPolygonRings((Polygon) geom));
      case "MultiPolygon" -> writeMultiPolygonShape((MultiPolygon) geom);
      case "GeometryCollection" -> writeGeometryCollection((GeometryCollection) geom);
      default -> throw new IllegalArgumentException("Unsupported geometry type: " + geom.getGeometryType());
    };
  }

  private static byte[] writeLineString(LineString ls) {
    Coordinate[] coords = ls.getCoordinates();
    Coordinate[][] parts = {coords};
    return writePolylineBuffer(parts, coordsHaveZ(coords));
  }

  private static byte[] writeMultiLineString(MultiLineString mls) {
    Coordinate[][] parts = new Coordinate[mls.getNumGeometries()][];
    boolean hasZ = false;
    for (int i = 0; i < parts.length; i++) {
      parts[i] = mls.getGeometryN(i).getCoordinates();
      if (!hasZ && coordsHaveZ(parts[i])) {
        hasZ = true;
      }
    }
    return writePolylineBuffer(parts, hasZ);
  }

  private static byte[] writeMultiPolygonShape(MultiPolygon mp) {
    List<Coordinate[]> allRings = new ArrayList<>();
    for (int i = 0; i < mp.getNumGeometries(); i++) {
      allRings.addAll(collectPolygonRings((Polygon) mp.getGeometryN(i)));
    }
    return writePolygonBuffer(allRings);
  }

  private static byte[] writeGeometryCollection(GeometryCollection gc) {
    if (gc.getNumGeometries() == 0) {
      ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      buf.putInt(TYPE_NULL);
      return buf.array();
    }
    throw new IllegalArgumentException(
        "Heterogeneous GeometryCollection is not supported by ESRI shape format");
  }

  private static byte[] writePoint(Point point) {
    Coordinate c = point.getCoordinate();
    boolean hasZ = !Double.isNaN(c.getZ());
    if (hasZ) {
      ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
      buf.putInt(TYPE_POINT_Z);
      buf.putDouble(c.getX());
      buf.putDouble(c.getY());
      buf.putDouble(c.getZ());
      return buf.array();
    } else {
      ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
      buf.putInt(TYPE_POINT);
      buf.putDouble(c.getX());
      buf.putDouble(c.getY());
      return buf.array();
    }
  }

  private static byte[] writeMultiPoint(MultiPoint mp) {
    int n = mp.getNumGeometries();
    Coordinate[] coords = new Coordinate[n];
    for (int i = 0; i < n; i++) {
      coords[i] = mp.getGeometryN(i).getCoordinate();
    }
    boolean hasZ = coordsHaveZ(coords);

    double[] bbox = bbox2D(coords);
    // type(4) + bbox(32) + numPoints(4) + points*16 [+ zMin+zMax(16) + n*8 if Z]
    int baseSize = 4 + 32 + 4 + n * 16;
    int size = hasZ ? baseSize + 16 + n * 8 : baseSize;
    int shapeType = hasZ ? TYPE_MULTIPOINT_Z : TYPE_MULTIPOINT;

    ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(shapeType);
    writeBbox(buf, bbox);
    buf.putInt(n);
    for (Coordinate c : coords) {
      buf.putDouble(c.getX());
      buf.putDouble(c.getY());
    }
    if (hasZ) {
      writeZSection(buf, coords);
    }
    return buf.array();
  }

  /**
   * Write Polyline (type 3 or 13) from an array of coordinate rings/paths.
   */
  private static byte[] writePolylineBuffer(Coordinate[][] parts, boolean hasZ) {
    int numParts  = parts.length;
    int numPoints = 0;
    for (Coordinate[] part : parts) {
      numPoints += part.length;
    }

    Coordinate[] allCoords = flattenParts(parts);
    double[] bbox = bbox2D(allCoords);
    int shapeType = hasZ ? TYPE_POLYLINE_Z : TYPE_POLYLINE;

    // type(4) + bbox(32) + numParts(4) + numPoints(4) + parts*4 + points*16 [+ Z section]
    int baseSize = 4 + 32 + 4 + 4 + numParts * 4 + numPoints * 16;
    int size = hasZ ? baseSize + 16 + numPoints * 8 : baseSize;

    ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(shapeType);
    writeBbox(buf, bbox);
    buf.putInt(numParts);
    buf.putInt(numPoints);

    int offset = 0;
    for (Coordinate[] part : parts) {
      buf.putInt(offset);
      offset += part.length;
    }
    for (Coordinate c : allCoords) {
      buf.putDouble(c.getX());
      buf.putDouble(c.getY());
    }
    if (hasZ) {
      writeZSection(buf, allCoords);
    }
    return buf.array();
  }

  /**
   * Collect all rings from a Polygon as Coordinate arrays, ensuring exterior rings are
   * clockwise and holes are counterclockwise (ESRI convention).
   */
  private static List<Coordinate[]> collectPolygonRings(Polygon polygon) {
    List<Coordinate[]> rings = new ArrayList<>();
    Coordinate[] shellCoords = polygon.getExteriorRing().getCoordinates();
    // Exterior ring must be clockwise (ESRI convention)
    if (Orientation.isCCW(shellCoords)) {
      shellCoords = reverse(shellCoords);
    }
    rings.add(shellCoords);

    for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
      Coordinate[] holeCoords = polygon.getInteriorRingN(h).getCoordinates();
      // Hole must be counterclockwise (ESRI convention)
      if (!Orientation.isCCW(holeCoords)) {
        holeCoords = reverse(holeCoords);
      }
      rings.add(holeCoords);
    }
    return rings;
  }

  /**
   * Write Polygon (type 5 or 15) from a list of ring coordinate arrays.
   * The first ring in each polygon group must be the exterior (clockwise), followed by
   * its holes (counterclockwise). This method accepts already-oriented rings.
   */
  private static byte[] writePolygonBuffer(List<Coordinate[]> rings) {
    int numParts  = rings.size();
    int numPoints = 0;
    for (Coordinate[] ring : rings) {
      numPoints += ring.length;
    }

    Coordinate[] allCoords = new Coordinate[numPoints];
    int pos = 0;
    for (Coordinate[] ring : rings) {
      System.arraycopy(ring, 0, allCoords, pos, ring.length);
      pos += ring.length;
    }

    boolean hasZ = coordsHaveZ(allCoords);
    double[] bbox = bbox2D(allCoords);
    int shapeType = hasZ ? TYPE_POLYGON_Z : TYPE_POLYGON;

    int baseSize = 4 + 32 + 4 + 4 + numParts * 4 + numPoints * 16;
    int size = hasZ ? baseSize + 16 + numPoints * 8 : baseSize;

    ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(shapeType);
    writeBbox(buf, bbox);
    buf.putInt(numParts);
    buf.putInt(numPoints);

    int offset = 0;
    for (Coordinate[] ring : rings) {
      buf.putInt(offset);
      offset += ring.length;
    }
    for (Coordinate c : allCoords) {
      buf.putDouble(c.getX());
      buf.putDouble(c.getY());
    }
    if (hasZ) {
      writeZSection(buf, allCoords);
    }
    return buf.array();
  }

  /** Write a 4-double bounding box into {@code buf}. */
  private static void writeBbox(ByteBuffer buf, double[] bbox) {
    buf.putDouble(bbox[0]); // xmin
    buf.putDouble(bbox[1]); // ymin
    buf.putDouble(bbox[2]); // xmax
    buf.putDouble(bbox[3]); // ymax
  }

  /**
   * Write the Z section: zMin (double), zMax (double), then one Z per coordinate.
   * NaN Z values are written as 0.0.
   */
  private static void writeZSection(ByteBuffer buf, Coordinate[] coords) {
    double zMin = Double.MAX_VALUE;
    double zMax = -Double.MAX_VALUE;
    for (Coordinate c : coords) {
      double z = Double.isNaN(c.getZ()) ? 0.0 : c.getZ();
      if (z < zMin) {
        zMin = z;
      }
      if (z > zMax) {
        zMax = z;
      }
    }
    buf.putDouble(zMin);
    buf.putDouble(zMax);
    for (Coordinate c : coords) {
      buf.putDouble(Double.isNaN(c.getZ()) ? 0.0 : c.getZ());
    }
  }

  /** Reverse a coordinate array (returns a new array, does not modify the original). */
  private static Coordinate[] reverse(Coordinate[] coords) {
    Coordinate[] reversed = new Coordinate[coords.length];
    for (int i = 0; i < coords.length; i++) {
      reversed[i] = coords[coords.length - 1 - i];
    }
    return reversed;
  }

  /** Return true if any coordinate in the array has a non-NaN Z value. */
  private static boolean coordsHaveZ(Coordinate[] coords) {
    for (Coordinate c : coords) {
      if (!Double.isNaN(c.getZ())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compute [xmin, ymin, xmax, ymax] bounding box from coordinates.
   * Ignores Z.
   */
  private static double[] bbox2D(Coordinate[] coords) {
    double xmin = Double.MAX_VALUE;
    double ymin = Double.MAX_VALUE;
    double xmax = -Double.MAX_VALUE;
    double ymax = -Double.MAX_VALUE;
    for (Coordinate c : coords) {
      if (c.getX() < xmin) {
        xmin = c.getX();
      }
      if (c.getX() > xmax) {
        xmax = c.getX();
      }
      if (c.getY() < ymin) {
        ymin = c.getY();
      }
      if (c.getY() > ymax) {
        ymax = c.getY();
      }
    }
    return new double[] {xmin, ymin, xmax, ymax};
  }

  /** Read {@code n} (x, y) coordinate pairs from {@code buf} into a Coordinate array. */
  private static Coordinate[] readXYCoords(ByteBuffer buf, int n) {
    Coordinate[] coords = new Coordinate[n];
    for (int i = 0; i < n; i++) {
      coords[i] = new Coordinate(buf.getDouble(), buf.getDouble());
    }
    return coords;
  }

  /**
   * Read Z values from {@code buf} into an existing coordinate array.
   * Skips zMin and zMax (2 doubles) before reading the per-coordinate Z values.
   */
  private static void readZIntoCoords(ByteBuffer buf, Coordinate[] coords) {
    // skip zMin, zMax
    buf.position(buf.position() + 16);
    for (Coordinate coord : coords) {
      coord.setZ(buf.getDouble());
    }
  }

  /** Copy a slice of a coordinate array. */
  private static Coordinate[] copyRange(Coordinate[] src, int from, int to) {
    Coordinate[] dst = new Coordinate[to - from];
    System.arraycopy(src, from, dst, 0, to - from);
    return dst;
  }

  /** Flatten an array of coordinate arrays into a single coordinate array. */
  private static Coordinate[] flattenParts(Coordinate[][] parts) {
    int total = 0;
    for (Coordinate[] part : parts) {
      total += part.length;
    }
    Coordinate[] all = new Coordinate[total];
    int pos = 0;
    for (Coordinate[] part : parts) {
      System.arraycopy(part, 0, all, pos, part.length);
      pos += part.length;
    }
    return all;
  }
}
