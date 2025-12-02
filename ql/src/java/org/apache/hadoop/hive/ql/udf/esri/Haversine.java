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

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

import com.esri.core.geometry.ogc.OGCPoint;

// Class is based on Apache Sedona code:
// https://github.com/apache/sedona/blob/eee44b509624d9e4022a6dd40d9b07d72a369a20/common/src/main/java/org/apache/sedona/common/sphere/Haversine.java#L43
public final class Haversine {
  public static final double AVG_EARTH_RADIUS_METERS = 6371008.0;

  private Haversine() {}

  /**
   * Calculate the distance between two points on the earth using the "haversine" formula. This is
   * also known as the great-circle distance This will produce almost identical result to PostGIS
   * ST_DistanceSphere and ST_Distance(useSpheroid=false)
   */
  public static double distance(double lon1, double lat1,
                                double lon2, double lat2, double radius) {
    double latDistance = toRadians(lat2 - lat1);
    double lngDistance = toRadians(lon2 - lon1);
    double a =
        sin(latDistance / 2) * sin(latDistance / 2)
            + cos(toRadians(lat1))
                * cos(toRadians(lat2))
                * sin(lngDistance / 2)
                * sin(lngDistance / 2);
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));
    return radius * c;
  }

  public static double distanceMeters(double lon1, double lat1,
                                       double lon2, double lat2) {
    return distance(lon1, lat1, lon2, lat2, AVG_EARTH_RADIUS_METERS);
  }

  public static double distanceMeters(OGCPoint point1, OGCPoint point2) {
    double lon1 = point1.X();
    double lat1 = point1.Y();
    double lon2 = point2.X();
    double lat2 = point2.Y();
    return distanceMeters(lon1, lat1, lon2, lat2);
  }
}
