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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_GeodesicLengthWGS84",
    value = "_FUNC_(line) - returns distance along line on WGS84 spheroid, in meters, for geographic coordinates",
    extended = "Requires the geometry to be in in WGS84 spatial reference, else returns NULL\nExample:\n"
        + " SELECT _FUNC_(ST_SetSRID(ST_Linestring(0.0,0.0, 0.3,0.4), 4326)) FROM src LIMIT 1; -- 55km\n"
        + " SELECT _FUNC_(ST_GeomFromText('MultiLineString((0.0 80.0, 0.3 80.4))', 4326)) FROM src LIMIT 1; -- 45km\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select substr(ST_GeodesicLengthWGS84(ST_GeomFromText('LineString(0 0, 0.03 0.04)', 4326)), 1, 5) from onerow",
//			result = "5542."
//			),
//		@HivePdkUnitTest(
//			query = "select substr(ST_GeodesicLengthWGS84(ST_GeomFromText('MultiLineString((0 80, 0.03 80.04))', 4326)), 1, 5) from onerow",
//			result = "4503."
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Length(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_GeodesicLengthWGS84 extends ST_GeometryAccessor {
  final DoubleWritable resultDouble = new DoubleWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_GeodesicLengthWGS84.class.getName());

  public DoubleWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    int WGS84 = 4326;
    if (GeometryUtils.getWKID(geomref) != WGS84) {
      LogUtils.Log_SRIDMismatch(LOG, geomref, WGS84);
      return null;
    }

    Geometry geom = GeometryUtils.geometryFromEsriShape(geomref);
    if (geom == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    switch (geom.getGeometryType()) {
      case "Point", "MultiPoint" -> resultDouble.set(0.0);
      case "LineString", "LinearRing" -> resultDouble.set(lineStringLength((LineString) geom));
      case "MultiLineString" -> {
        MultiLineString mls = (MultiLineString) geom;
        double total = 0.0;
        for (int i = 0; i < mls.getNumGeometries(); i++) {
          total += lineStringLength((LineString) mls.getGeometryN(i));
        }
        resultDouble.set(total);
      }
      default -> resultDouble.set(0.0);
    }

    return resultDouble;
  }

  /**
   * Computes the geodesic length of a single LineString by summing Haversine
   * distances between consecutive vertices.  Iterates the CoordinateSequence
   * directly to avoid per-vertex Point object allocation.
   */
  private static double lineStringLength(LineString line) {
    CoordinateSequence seq = line.getCoordinateSequence();
    int nPts = seq.size();
    if (nPts < 2) {
      return 0.0;
    }
    double length = 0.0;
    for (int i = 1; i < nPts; i++) {
      length += Haversine.distanceMeters(
          seq.getX(i - 1), seq.getY(i - 1),
          seq.getX(i), seq.getY(i));
    }
    return length;
  }
}
