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

  // WGS84 ellipsoid parameters.
  private static final double WGS84_A = 6378137.0;             // semi-major axis (meters)
  private static final double WGS84_F = 1.0 / 298.257223563;   // flattening
  private static final double WGS84_B = WGS84_A * (1.0 - WGS84_F);  // semi-minor axis (meters)

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
   * Computes the geodesic length of a single LineString by summing the ellipsoidal
   * (WGS84) distances between consecutive vertices.  Iterates the CoordinateSequence
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
      length += vincentyDistanceMeters(
          seq.getX(i - 1), seq.getY(i - 1),
          seq.getX(i), seq.getY(i));
    }
    return length;
  }

  /**
   * Ellipsoidal geodesic distance between two lon/lat points on the WGS84 spheroid,
   * using Vincenty's inverse formula (the same ellipsoidal model the ESRI library used,
   * so results match the previous implementation).  Coordinates are in degrees, result
   * in meters.
   */
  static double vincentyDistanceMeters(double lon1, double lat1, double lon2, double lat2) {
    double l = Math.toRadians(lon2 - lon1);
    double u1 = Math.atan((1.0 - WGS84_F) * Math.tan(Math.toRadians(lat1)));
    double u2 = Math.atan((1.0 - WGS84_F) * Math.tan(Math.toRadians(lat2)));
    double sinU1 = Math.sin(u1);
    double cosU1 = Math.cos(u1);
    double sinU2 = Math.sin(u2);
    double cosU2 = Math.cos(u2);

    double lambda = l;
    double sinSigma = 0.0;
    double cosSigma = 0.0;
    double sigma = 0.0;
    double cosSqAlpha = 0.0;
    double cos2SigmaM = 0.0;

    for (int iter = 0; iter < 200; iter++) {
      double sinLambda = Math.sin(lambda);
      double cosLambda = Math.cos(lambda);
      sinSigma = Math.sqrt(Math.pow(cosU2 * sinLambda, 2)
          + Math.pow(cosU1 * sinU2 - sinU1 * cosU2 * cosLambda, 2));
      if (sinSigma == 0.0) {
        return 0.0;  // coincident points
      }
      cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
      sigma = Math.atan2(sinSigma, cosSigma);
      double sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
      cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
      // equatorial line: cosSqAlpha == 0, guard the cos2SigmaM division
      cos2SigmaM = (cosSqAlpha == 0.0) ? 0.0 : cosSigma - 2.0 * sinU1 * sinU2 / cosSqAlpha;
      double c = WGS84_F / 16.0 * cosSqAlpha * (4.0 + WGS84_F * (4.0 - 3.0 * cosSqAlpha));
      double lambdaPrev = lambda;
      lambda = l + (1.0 - c) * WGS84_F * sinAlpha
          * (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)));
      if (Math.abs(lambda - lambdaPrev) < 1e-12) {
        break;
      }
    }

    double uSq = cosSqAlpha * (WGS84_A * WGS84_A - WGS84_B * WGS84_B) / (WGS84_B * WGS84_B);
    double a = 1.0 + uSq / 16384.0 * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
    double b = uSq / 1024.0 * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));
    double deltaSigma = b * sinSigma * (cos2SigmaM + b / 4.0
        * (cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)
        - b / 6.0 * cos2SigmaM * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SigmaM * cos2SigmaM)));
    return WGS84_B * a * (sigma - deltaSigma);
  }
}
