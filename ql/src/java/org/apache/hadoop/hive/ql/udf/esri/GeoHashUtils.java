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
package org.apache.hadoop.hive.ql.udf.esri;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;

public final class GeoHashUtils {

  public static final int MIN_CHARACTER_PRECISION = 1;

  /**
   * Maximum geohash length in base-32 characters for {@link GeoHash#geoHashStringWithCharacterPrecision}.
   */
  public static final int MAX_CHARACTER_PRECISION = 12;

  public static final int DEFAULT_CHARACTER_PRECISION = 12;

  private GeoHashUtils() {
  }

  public static String geohashForPoint(double longitude, double latitude, int characterPrecision) {
    return GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, characterPrecision);
  }

  /**
   * Returns a rectangular polygon for the geohash cell (closed ring, lon/lat coordinates).
   *
   * @param geohash base-32 geohash string (non-empty)
   * @param characterPrecision number of leading characters to use (1–12, at most {@code geohash}
   *     length)
   */
  public static Polygon geohashCellPolygon(String geohash, int characterPrecision) {
    if (geohash == null || geohash.isEmpty()) {
      return null;
    }
    if (characterPrecision < MIN_CHARACTER_PRECISION ||
        characterPrecision > MAX_CHARACTER_PRECISION ||
        characterPrecision > geohash.length()) {
      return null;
    }
    String hashPrefix = geohash.substring(0, characterPrecision);
    BoundingBox box = GeoHash.fromGeohashString(hashPrefix).getBoundingBox();
    double west = box.getWestLongitude();
    double east = box.getEastLongitude();
    double south = box.getSouthLatitude();
    double north = box.getNorthLatitude();
    // Closed ring (west,south) -> (west,north) -> (east,north) -> (east,south) -> close.
    Coordinate[] ring = new Coordinate[] {
        new Coordinate(west, south),
        new Coordinate(west, north),
        new Coordinate(east, north),
        new Coordinate(east, south),
        new Coordinate(west, south)
    };
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(ring);
  }

  public static int resolveEncodePrecision(Integer precisionArg) {
    int precision = precisionArg == null ? DEFAULT_CHARACTER_PRECISION : precisionArg;
    if (precision < MIN_CHARACTER_PRECISION || precision > MAX_CHARACTER_PRECISION) {
      return -1;
    }
    return precision;
  }

  public static int resolveDecodePrecision(Integer precisionArg, int geohashLength) {
    int precision = precisionArg == null ? geohashLength : precisionArg;
    if (precision < MIN_CHARACTER_PRECISION ||
        precision > geohashLength ||
        geohashLength > MAX_CHARACTER_PRECISION) {
      return -1;
    }
    return precision;
  }
}
