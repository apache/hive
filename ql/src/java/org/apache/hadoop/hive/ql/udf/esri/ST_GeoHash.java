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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(
    name = "ST_GeoHash",
    value = """
        _FUNC_(point) - geohash for a point geometry
        _FUNC_(point, precision) - geohash at given character precision
        """,
    extended = """
        SELECT _FUNC_(ST_Point(-126.965375, 43.234528), 12);  -- 9pttyydekk4t
        """)
public class ST_GeoHash extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_GeoHash.class.getName());

  public Text evaluate(BytesWritable geomref) {
    return evaluate(geomref, null);
  }

  public Text evaluate(BytesWritable geomref, IntWritable precisionArg) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    GeometryUtils.OGCType type = GeometryUtils.getType(geomref);
    if (type != GeometryUtils.OGCType.ST_POINT) {
      LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, type);
      return null;
    }

    Geometry geom = GeometryUtils.geometryFromEsriShape(geomref);
    if (geom == null) {
      return null;
    }
    Point point = (Point) geom;
    return geohashText(point.getX(), point.getY(), precisionArg);
  }

  private Text geohashText(double longitude, double latitude, IntWritable precisionArg) {
    int precision =
        GeoHashUtils.resolveEncodePrecision(precisionArg == null ? null : precisionArg.get());
    if (precision < 0) {
      LogUtils.Log_InvalidPrecision(LOG, GeoHashUtils.MIN_CHARACTER_PRECISION,
          GeoHashUtils.MAX_CHARACTER_PRECISION);
      return null;
    }
    try {
      String hash = GeoHashUtils.geohashForPoint(longitude, latitude, precision);
      return new Text(hash);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_GeoHash: " + e);
      return null;
    }
  }
}
