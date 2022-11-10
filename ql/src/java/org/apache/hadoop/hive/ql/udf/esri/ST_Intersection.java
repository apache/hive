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

import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Intersection",
    value = "_FUNC_(ST_Geometry1, ST_Geometry2) - intersection of ST_Geometry1 & ST_Geometry2",
    extended = "Example:\n" + "  SELECT ST_AsText(_FUNC_(ST_Point(1,1), ST_Point(1,1))) FROM onerow; -- POINT (1 1)\n"
        + "  SELECT ST_AsText(_FUNC_(ST_GeomFromText('linestring(0 2, 0 0, 2 0)'), ST_GeomFromText('linestring(0 3, 0 1, 1 0, 3 0)'))) FROM onerow; -- MULTILINESTRING ((1 0, 2 0), (0 2, 0 1))\n"
        + "  SELECT ST_AsText(_FUNC_(ST_LineString(0,2, 2,3), ST_Polygon(1,1, 4,1, 4,4, 1,4))) FROM onerow; -- MULTILINESTRING ((1 2.5, 2 3))\n"
        + "  SELECT ST_AsText(_FUNC_(ST_Polygon(2,0, 2,3, 3,0), ST_Polygon(1,1, 4,1, 4,4, 1,4))) FROM onerow; -- MULTIPOLYGON (((2.67 1, 2 3, 2 1, 2.67 1)))\n"
        + "OGC Compliance Notes : \n" + " In the case where the two geometries intersect in a lower dimension,"
        + " ST_Intersection may drop the lower-dimension intersections, or output a closed linestring.\n"
        + "SELECT ST_AsText(_FUNC_(ST_Polygon(2,0, 3,1, 2,1), ST_Polygon(1,1, 4,1, 4,4, 1,4))) FROM onerow; -- MULTIPOLYGON EMPTY or LINESTRING (2 1, 3 1, 2 1)\n")

public class ST_Intersection extends ST_GeometryProcessing {
  static final Logger LOG = LoggerFactory.getLogger(ST_Intersection.class.getName());

  public BytesWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {
    if (geometryref1 == null || geometryref2 == null || geometryref1.getLength() == 0
        || geometryref2.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }
    if (!GeometryUtils.compareSpatialReferences(geometryref1, geometryref2)) {
      LogUtils.Log_SRIDMismatch(LOG, geometryref1, geometryref2);
      return null;
    }

    OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geometryref1);
    OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geometryref2);
    if (ogcGeom1 == null || ogcGeom2 == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry commonGeom;
    try {
      commonGeom = ogcGeom1.intersection(ogcGeom2);
      return GeometryUtils.geometryToEsriShapeBytesWritable(commonGeom);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_Intersection: " + e);
      return null;
    }
  }

}
