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

import com.esri.core.geometry.ogc.OGCPoint;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.esri.GeometryUtils.OGCType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_DistanceSphere",
    value = "_FUNC_(ST_Geometry1, ST_Geometry2) - returns the great circle distance between 2 points in meters",
    extended = "Example:\n" +
       "  SELECT _FUNC_(ST_Point(113.914603,22.308919), ST_Point(151.177222,-33.946111)) FROM src LIMIT 1;\n" +
       "  --  ~7393893")
public class ST_DistanceSphere extends ST_GeometryAccessor {
  final DoubleWritable resultDouble = new DoubleWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_DistanceSphere.class.getName());

  public DoubleWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {
    if (geometryref1 == null || geometryref2 == null || geometryref1.getLength() == 0
        || geometryref2.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }
    if (!GeometryUtils.compareSpatialReferences(geometryref1, geometryref2)) {
      LogUtils.Log_SRIDMismatch(LOG, geometryref1, geometryref2);
      return null;
    }
    // Only distance of points is supported.
    if (GeometryUtils.getType(geometryref1) != OGCType.ST_POINT) {
      LogUtils.Log_InvalidType(LOG, OGCType.ST_POINT, GeometryUtils.getType(geometryref1));
      return null;
    }
    if (GeometryUtils.getType(geometryref2) != OGCType.ST_POINT) {
      LogUtils.Log_InvalidType(LOG, OGCType.ST_POINT, GeometryUtils.getType(geometryref2));
      return null;
    }

    OGCPoint point1 = (OGCPoint) GeometryUtils.geometryFromEsriShape(geometryref1);
    OGCPoint point2 = (OGCPoint) GeometryUtils.geometryFromEsriShape(geometryref2);
    if (point1 == null || point2 == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    try {
      resultDouble.set(Haversine.distanceMeters(point1, point2));
      return resultDouble;
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_DistanceSphere: " + e);
      return null;
    }
  }
}
