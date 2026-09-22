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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(
    name = "ST_GeomFromGeoHash",
    value = """
        _FUNC_(geohash) - polygon for the geohash cell
        _FUNC_(geohash, precision) - polygon using the first precision characters
        """,
    extended = """
        SELECT ST_AsText(_FUNC_('9ptty', 5));
        """)
public class ST_GeomFromGeoHash extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_GeomFromGeoHash.class.getName());

  public BytesWritable evaluate(Text geohashText) {
    return evaluate(geohashText, null);
  }

  public BytesWritable evaluate(Text geohashText, IntWritable precisionArg) {
    String geohash = geohashText != null ? geohashText.toString().trim() : null;
    if (geohash == null || geohash.isEmpty()) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    int characterPrecision = GeoHashUtils.resolveDecodePrecision(precisionArg == null ? null : precisionArg.get(),
        geohash.length());
    if (characterPrecision < 0) {
      LogUtils.Log_InvalidPrecision(LOG, GeoHashUtils.MIN_CHARACTER_PRECISION,
          Math.min(geohash.length(), GeoHashUtils.MAX_CHARACTER_PRECISION));
      return null;
    }

    try {
      Polygon polygon = GeoHashUtils.geohashCellPolygon(geohash, characterPrecision);
      if (polygon == null) {
        return null;
      }
      return GeometryUtils.geometryToEsriShapeBytesWritable(polygon);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_GeomFromGeoHash: " + e);
      return null;
    }
  }
}
