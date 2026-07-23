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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Set;

public class GeometryUtils {

  private static final Logger LOG = LoggerFactory.getLogger(GeometryUtils.class);

  private static final int SIZE_WKID = 4;
  private static final int SIZE_TYPE = 1;

  public static final int WKID_UNKNOWN = 0;

  // Magic byte at offset 4 to distinguish new WKB format from old ESRI format.
  // Old format stores OGC type tag (0-6) at byte 4; 0xFF is outside that range.
  public static final byte NEW_FORMAT_MAGIC = (byte) 0xFF;

  public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  // ThreadLocal JTS readers/writers to avoid per-row allocation overhead.
  // These classes are not thread-safe, so ThreadLocal provides safe reuse
  // across millions of rows within the same task thread.
  private static final ThreadLocal<WKBReader> WKB_READER =
      ThreadLocal.withInitial(() -> new WKBReader(GEOMETRY_FACTORY));
  private static final ThreadLocal<WKBWriter> WKB_WRITER =
      ThreadLocal.withInitial(WKBWriter::new);
  private static final ThreadLocal<WKBWriter> WKB_WRITER_3D =
      ThreadLocal.withInitial(() -> new WKBWriter(3));
  private static final ThreadLocal<WKBWriter> WKB_WRITER_XYM = ThreadLocal.withInitial(() -> {
    WKBWriter w = new WKBWriter(3);
    w.setOutputOrdinates(Ordinate.createXYM());
    return w;
  });
  private static final ThreadLocal<WKBWriter> WKB_WRITER_4D =
      ThreadLocal.withInitial(() -> new WKBWriter(4));
  private static final ThreadLocal<WKTReader> WKT_READER =
      ThreadLocal.withInitial(() -> new WKTReader(GEOMETRY_FACTORY));
  private static final ThreadLocal<WKTWriter> WKT_WRITER =
      ThreadLocal.withInitial(WKTWriter::new);
  private static final ThreadLocal<WKTWriter> WKT_WRITER_3D =
      ThreadLocal.withInitial(() -> new WKTWriter(3));
  private static final ThreadLocal<WKTWriter> WKT_WRITER_XYM = ThreadLocal.withInitial(() -> {
    WKTWriter w = new WKTWriter(3);
    w.setOutputOrdinates(Ordinate.createXYM());
    return w;
  });
  private static final ThreadLocal<WKTWriter> WKT_WRITER_4D =
      ThreadLocal.withInitial(() -> new WKTWriter(4));
  private static final ThreadLocal<GeoJsonReader> GEOJSON_READER =
      ThreadLocal.withInitial(GeoJsonReader::new);
  private static final ThreadLocal<GeoJsonWriter> GEOJSON_WRITER =
      ThreadLocal.withInitial(GeoJsonWriter::new);

  /** Get the thread-local WKBReader instance (reused across rows). */
  public static WKBReader wkbReader() {
    return WKB_READER.get();
  }

  /** Get the thread-local WKBWriter instance (reused across rows). */
  public static WKBWriter wkbWriter() {
    return WKB_WRITER.get();
  }

  /** Get the thread-local WKTReader instance (reused across rows). */
  public static WKTReader wktReader() {
    return WKT_READER.get();
  }

  /** Get the thread-local WKTWriter instance (reused across rows). */
  public static WKTWriter wktWriter() {
    return WKT_WRITER.get();
  }

  /** Get a dimension-aware WKBWriter that matches the geometry's coordinate type. */
  public static WKBWriter wkbWriterFor(Geometry geom) {
    Set<Ordinate> ordinates = getOrdinates(geom);
    if (is4D(ordinates)) {
      return WKB_WRITER_4D.get();
    } else if (is3D(ordinates)) {
      return WKB_WRITER_3D.get();
    } else if (isMeasured(ordinates)) {
      return WKB_WRITER_XYM.get();
    }
    return WKB_WRITER.get();
  }

  /** Get a dimension-aware WKTWriter that matches the geometry's coordinate type. */
  public static WKTWriter wktWriterFor(Geometry geom) {
    Set<Ordinate> ordinates = getOrdinates(geom);
    if (is4D(ordinates)) {
      return WKT_WRITER_4D.get();
    } else if (is3D(ordinates)) {
      return WKT_WRITER_3D.get();
    } else if (isMeasured(ordinates)) {
      return WKT_WRITER_XYM.get();
    }
    return WKT_WRITER.get();
  }

  /** True if the geometry carries Z values. */
  public static boolean is3D(Geometry geom) {
    return is3D(getOrdinates(geom));
  }

  /** True if the geometry carries M (measure) values. */
  public static boolean isMeasured(Geometry geom) {
    return isMeasured(getOrdinates(geom));
  }

  /** True if the ordinates include both Z and M. */
  public static boolean is4D(Set<Ordinate> ordinates) {
    return is3D(ordinates) && isMeasured(ordinates);
  }

  /** True if the ordinates include Z. */
  public static boolean is3D(Set<Ordinate> ordinates) {
    return ordinates.contains(Ordinate.Z);
  }

  /** True if the ordinates include M. */
  public static boolean isMeasured(Set<Ordinate> ordinates) {
    return ordinates.contains(Ordinate.M);
  }

  /**
   * Determine the output ordinates present in a geometry by inspecting
   * its coordinate sequences. Also validates that Z values are not NaN,
   * since JTS base Coordinate class reports dimension=3 even for 2D points.
   */
  public static Set<Ordinate> getOrdinates(Geometry geom) {
    CoordinateSequence seq = getFirstCoordinateSequence(geom);
    if (seq == null || seq.size() == 0) {
      return Ordinate.createXY();
    }
    boolean hasZ = seq.hasZ() && !Double.isNaN(seq.getZ(0));
    boolean hasM = seq.hasM();
    if (hasZ && hasM) {
      return Ordinate.createXYZM();
    } else if (hasZ) {
      return Ordinate.createXYZ();
    } else if (hasM) {
      return Ordinate.createXYM();
    }
    return Ordinate.createXY();
  }

  /**
   * Get the first non-empty CoordinateSequence from a geometry.
   */
  private static CoordinateSequence getFirstCoordinateSequence(Geometry geom) {
    if (geom == null || geom.isEmpty()) {
      return null;
    }
    if (geom instanceof Point point) {
      return point.getCoordinateSequence();
    }
    if (geom instanceof LineString lineString) {
      return lineString.getCoordinateSequence();
    }
    if (geom instanceof Polygon polygon) {
      return polygon.getExteriorRing().getCoordinateSequence();
    }
    if (geom instanceof GeometryCollection) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        CoordinateSequence seq = getFirstCoordinateSequence(geom.getGeometryN(i));
        if (seq != null) {
          return seq;
        }
      }
    }
    return null;
  }

  /** Get the thread-local GeoJsonReader instance (reused across rows). */
  public static GeoJsonReader geoJsonReader() {
    return GEOJSON_READER.get();
  }

  /** Get the thread-local GeoJsonWriter instance (reused across rows). */
  public static GeoJsonWriter geoJsonWriter() {
    return GEOJSON_WRITER.get();
  }

  /**
   * Remove all ThreadLocal reader/writer instances for the current thread.
   * Call this during UDF close() to prevent memory leaks in long-lived
   * thread pools (e.g., HiveServer2 fetch tasks, constant folding).
   */
  public static void cleanup() {
    WKB_READER.remove();
    WKB_WRITER.remove();
    WKB_WRITER_3D.remove();
    WKB_WRITER_XYM.remove();
    WKB_WRITER_4D.remove();
    WKT_READER.remove();
    WKT_WRITER.remove();
    WKT_WRITER_3D.remove();
    WKT_WRITER_XYM.remove();
    WKT_WRITER_4D.remove();
    GEOJSON_READER.remove();
    GEOJSON_WRITER.remove();
  }

  public enum OGCType {
    UNKNOWN(0),
    ST_POINT(1),
    ST_LINESTRING(2),
    ST_POLYGON(3),
    ST_MULTIPOINT(4),
    ST_MULTILINESTRING(5),
    ST_MULTIPOLYGON(6);

    private final int index;

    OGCType(int index) {
      this.index = index;
    }

    public int getIndex() {
      return this.index;
    }
  }

  public static OGCType[] OGCTypeLookup =
      { OGCType.UNKNOWN, OGCType.ST_POINT, OGCType.ST_LINESTRING, OGCType.ST_POLYGON, OGCType.ST_MULTIPOINT,
          OGCType.ST_MULTILINESTRING, OGCType.ST_MULTIPOLYGON };

  public static final WritableBinaryObjectInspector geometryTransportObjectInspector =
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

  private static final Cache<BytesWritable, Geometry> GEOMETRY_CACHE =
      CacheBuilder.newBuilder().maximumSize(1024).build();

  /**
   * Detect whether the given bytes use the new WKB format.
   */
  public static boolean isNewFormat(BytesWritable geomref) {
    return geomref != null && geomref.getLength() > 4 && geomref.getBytes()[4] == NEW_FORMAT_MAGIC;
  }

  /**
   * Compare spatial references of two geometry byte arrays.
   */
  public static boolean compareSpatialReferences(BytesWritable geomref1, BytesWritable geomref2) {
    return getWKID(geomref1) == getWKID(geomref2);
  }

  /**
   * Serialize a JTS Geometry to the new WKB wire format.
   * Normalizes polygon ring orientation to OGC standard (exterior CCW, interior CW).
   */
  public static BytesWritable geometryToEsriShapeBytesWritable(Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    geometry = normalizeRingOrientation(geometry);
    return new CachedGeometryBytesWritable(geometry);
  }

  /**
   * Serialize a JTS Geometry with a specific SRID.
   * Normalizes polygon ring orientation to OGC standard (exterior CCW, interior CW).
   */
  public static BytesWritable geometryToEsriShapeBytesWritable(Geometry geometry, int wkid) {
    if (geometry == null) {
      return null;
    }
    geometry = normalizeRingOrientation(geometry);
    geometry.setSRID(wkid);
    return new CachedGeometryBytesWritable(geometry);
  }

  /**
   * Normalize polygon ring orientation to OGC standard:
   * exterior rings counter-clockwise (CCW), interior rings clockwise (CW).
   * Non-polygon geometries are returned unchanged.
   */
  public static Geometry normalizeRingOrientation(Geometry geom) {
    if (geom instanceof Polygon polygon) {
      return normalizePolygonRings(polygon);
    } else if (geom instanceof MultiPolygon mp) {
      Polygon[] polys = new Polygon[mp.getNumGeometries()];
      for (int i = 0; i < mp.getNumGeometries(); i++) {
        polys[i] = normalizePolygonRings((Polygon) mp.getGeometryN(i));
      }
      MultiPolygon result = geom.getFactory().createMultiPolygon(polys);
      result.setSRID(geom.getSRID());
      return result;
    }
    return geom;
  }

  private static Polygon normalizePolygonRings(Polygon polygon) {
    GeometryFactory factory = polygon.getFactory();
    LinearRing shell = polygon.getExteriorRing();

    // Exterior ring should be CCW
    boolean shellChanged = false;
    if (shell.getNumPoints() >= 4 && !Orientation.isCCW(shell.getCoordinateSequence())) {
      shell = shell.reverse();
      shellChanged = true;
    }

    // Interior rings should be CW (not CCW)
    int numHoles = polygon.getNumInteriorRing();
    LinearRing[] holes = new LinearRing[numHoles];
    boolean holesChanged = false;
    for (int i = 0; i < numHoles; i++) {
      LinearRing hole = polygon.getInteriorRingN(i);
      if (hole.getNumPoints() >= 4 && Orientation.isCCW(hole.getCoordinateSequence())) {
        holes[i] = hole.reverse();
        holesChanged = true;
      } else {
        holes[i] = hole;
      }
    }

    if (!shellChanged && !holesChanged) {
      return polygon;
    }

    Polygon result = factory.createPolygon(shell, holes);
    result.setSRID(polygon.getSRID());
    return result;
  }

  /**
   * Deserialize geometry bytes (either old ESRI format or new WKB format) to a JTS Geometry.
   */
  public static Geometry geometryFromEsriShape(BytesWritable geomref) {
    return geometryFromEsriShape(geomref, true);
  }

  /**
   * Deserialize geometry bytes to a JTS Geometry.
   * @param geomref the geometry bytes
   * @param bytesRecycled true if the bytes backing the writable may be reused (disables caching)
   */
  public static Geometry geometryFromEsriShape(BytesWritable geomref, boolean bytesRecycled) {
    if (geomref == null) {
      return null;
    }

    if (geomref instanceof CachedGeometryBytesWritable cached) {
      return cached.getGeometry();
    }

    if (!bytesRecycled) {
      Geometry cachedGeom = GEOMETRY_CACHE.getIfPresent(geomref);
      if (cachedGeom != null) {
        return cachedGeom;
      }
    }

    int length = geomref.getLength();
    byte[] bytes = geomref.getBytes();

    if (length < 5) {
      return null;
    }

    Geometry geom;
    int srid;

    if (bytes[4] == NEW_FORMAT_MAGIC) {
      // New WKB format: bytes 0-3 = SRID (little-endian), byte 4 = 0xFF, bytes 5+ = WKB
      srid = ByteBuffer.wrap(bytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      byte[] wkb = Arrays.copyOfRange(bytes, 5, length);
      try {
        geom = wkbReader().read(wkb);
      } catch (ParseException e) {
        LOG.warn("Failed to parse WKB geometry", e);
        return null;
      }
    } else {
      // Old ESRI format: bytes 0-3 = WKID (little-endian), byte 4 = OGC type (0-6), bytes 5+ = ESRI shape
      srid = ByteBuffer.wrap(bytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      ByteBuffer shapeBuffer = ByteBuffer.wrap(bytes, SIZE_WKID + SIZE_TYPE, length - SIZE_WKID - SIZE_TYPE)
          .slice().order(ByteOrder.LITTLE_ENDIAN);

      if (shapeBuffer.limit() < 4) {
        return null;
      }

      // Check if shape type is Unknown (empty geometry)
      if (shapeBuffer.getInt(0) == 0) {
        return null;
      }

      // Use native EsriShapeConverter to read the old ESRI shape format
      try {
        geom = EsriShapeConverter.fromEsriShapeBody(shapeBuffer);
      } catch (Exception e) {
        LOG.warn("Failed to parse ESRI shape geometry", e);
        return null;
      }
    }

    if (geom != null) {
      geom.setSRID(srid);
      if (!bytesRecycled) {
        GEOMETRY_CACHE.put(geomref, geom);
      }
    }

    return geom;
  }

  /**
   * Gets the geometry type for the given hive geometry bytes.
   * For new WKB format, reads the type directly from the WKB header
   * without full deserialization.
   */
  public static OGCType getType(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() < 5) {
      return OGCType.UNKNOWN;
    }
    byte[] bytes = geomref.getBytes();
    if (bytes[4] == NEW_FORMAT_MAGIC) {
      // New format: bytes 5+ are WKB. WKB layout: byte 5 = byte order,
      // bytes 6-9 = geometry type int (in the indicated byte order).
      if (geomref.getLength() < 10) {
        return OGCType.UNKNOWN;
      }
      ByteOrder order = (bytes[5] == 1) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
      int wkbType = ByteBuffer.wrap(bytes, 6, 4).order(order).getInt();
      // JTS uses extended WKB: bit 31 (0x80000000) = Z, bit 30 (0x40000000) = M,
      // bit 29 (0x20000000) = SRID. Strip these flag bits to get the base type.
      // Also handle ISO WKB encoding (type + 1000/2000/3000) for compatibility.
      int baseType = wkbType & 0x1FFFFFFF; // strip Z/M/SRID flag bits
      if (baseType >= 1000) {
        baseType = baseType % 1000; // ISO WKB encoding
      }
      return switch (baseType) {
        case 1 -> OGCType.ST_POINT;
        case 2 -> OGCType.ST_LINESTRING;
        case 3 -> OGCType.ST_POLYGON;
        case 4 -> OGCType.ST_MULTIPOINT;
        case 5 -> OGCType.ST_MULTILINESTRING;
        case 6 -> OGCType.ST_MULTIPOLYGON;
        default -> OGCType.UNKNOWN;
      };
    }
    int typeIdx = bytes[SIZE_WKID] & 0xFF;
    if (typeIdx >= OGCTypeLookup.length) {
      return OGCType.UNKNOWN;
    }
    return OGCTypeLookup[typeIdx];
  }

  /**
   * Infer OGCType from a JTS Geometry.
   */
  public static OGCType inferOGCType(Geometry geom) {
    if (geom == null) {
      return OGCType.UNKNOWN;
    }
    return switch (geom.getGeometryType()) {
      case "Point" -> OGCType.ST_POINT;
      case "LineString", "LinearRing" -> OGCType.ST_LINESTRING;
      case "Polygon" -> OGCType.ST_POLYGON;
      case "MultiPoint" -> OGCType.ST_MULTIPOINT;
      case "MultiLineString" -> OGCType.ST_MULTILINESTRING;
      case "MultiPolygon" -> OGCType.ST_MULTIPOLYGON;
      default -> OGCType.UNKNOWN;
    };
  }

  /**
   * Gets the WKID/SRID for the given hive geometry bytes.
   */
  public static int getWKID(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() < 4) {
      return WKID_UNKNOWN;
    }
    // SRID/WKID is stored little-endian at bytes 0-3 in both the old and new formats.
    return ByteBuffer.wrap(geomref.getBytes(), 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  /**
   * Sets the WKID/SRID (in place) for the given hive geometry bytes.
   */
  public static void setWKID(BytesWritable geomref, int wkid) {
    // SRID/WKID is stored little-endian at bytes 0-3 in both the old and new formats.
    ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(wkid);
    System.arraycopy(bb.array(), 0, geomref.getBytes(), 0, SIZE_WKID);
  }

  /**
   * Serialize a JTS Geometry to the new WKB wire format bytes.
   * Uses dimension-aware WKBWriter to preserve Z/M coordinates.
   */
  private static BytesWritable serializeJts(Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    int srid = geometry.getSRID();
    byte[] wkb = wkbWriterFor(geometry).write(geometry);

    byte[] result = new byte[SIZE_WKID + 1 + wkb.length];
    // Write SRID little-endian (matches the old format's WKID byte order)
    result[0] = (byte) srid;
    result[1] = (byte) (srid >> 8);
    result[2] = (byte) (srid >> 16);
    result[3] = (byte) (srid >> 24);
    // Magic byte
    result[4] = NEW_FORMAT_MAGIC;
    // WKB payload
    System.arraycopy(wkb, 0, result, 5, wkb.length);
    return new BytesWritable(result);
  }

  /**
   * A BytesWritable that caches the JTS Geometry to avoid repeated deserialization.
   */
  public static class CachedGeometryBytesWritable extends BytesWritable {
    private final Geometry cachedGeom;

    public CachedGeometryBytesWritable(Geometry geom) {
      cachedGeom = geom;
      BytesWritable serialized = serializeJts(cachedGeom);
      if (serialized != null) {
        super.set(serialized);
      }
    }

    public Geometry getGeometry() {
      return cachedGeom;
    }
  }
}
