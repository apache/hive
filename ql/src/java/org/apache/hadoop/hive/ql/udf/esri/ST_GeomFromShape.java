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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.apache.hadoop.hive.ql.udf.esri.GeometryUtils.OGCType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_GeomFromShape",
    value = "_FUNC_(shape) - construct ST_Geometry from Esri shape representation of geometry\n",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_AsShape(ST_Point(1, 2))); -- constructs ST_Point\n") public class ST_GeomFromShape
    extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_GeomFromShape.class.getName());

  public BytesWritable evaluate(BytesWritable shape) throws UDFArgumentException {
    return evaluate(shape, 0);
  }

  public BytesWritable evaluate(BytesWritable shape, int wkid) throws UDFArgumentException {
    try {
      Geometry geometry = GeometryEngine.geometryFromEsriShape(shape.getBytes(), Geometry.Type.Unknown);
      switch (geometry.getType()) {
      case Point:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_POINT);

      case MultiPoint:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_MULTIPOINT);

      case Line:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_LINESTRING);

      case Polyline:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_MULTILINESTRING);

      case Envelope:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_POLYGON);

      case Polygon:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_MULTIPOLYGON);

      default:
        return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.UNKNOWN);
      }
    } catch (Exception e) {
      LogUtils.Log_ExceptionThrown(LOG, "geom-from-shape", e);
      return null;
    }
  }

}
