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

@Description(name = "ST_SymmetricDiff",
    value = "_FUNC_(ST_Geometry1, ST_Geometry2) - return the symmetric difference between ST_Geometry1 & ST_Geometry2",
    extended = "Examples:\n"
        + " > SELECT ST_AsText(_FUNC_(ST_LineString('linestring(0 2, 2 2)'), ST_LineString('linestring(1 2, 3 2)'))) FROM onerow; \n"
        + " MULTILINESTRING((0 2, 1 2), (2 2, 3 2))\n"
        + " > SELECT ST_AsText(_FUNC_(ST_SymmetricDiff(ST_Polygon('polygon((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_Polygon('polygon((1 1, 3 1, 3 3, 1 3, 1 1))'))) from onerow;\n"
        + " MULTIPOLYGON (((0 0, 2 0, 2 1, 1 1, 1 2, 0 2, 0 0)), ((3 1, 3 3, 1 3, 1 2, 2 2, 2 1, 3 1)))\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "SELECT ST_Equals(ST_SymmetricDiff(ST_LineString('linestring(0 2, 2 2)'), ST_LineString('linestring(1 2, 3 2)')), ST_GeomFromText('multilinestring((0 2, 1 2), (2 2, 3 2))')) FROM onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_Equals(ST_SymmetricDiff(ST_Polygon('polygon((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_Polygon('polygon((1 1, 3 1, 3 3, 1 3, 1 1))')), ST_MultiPolygon('multipolygon(((0 0, 2 0, 2 1, 1 1, 1 2, 0 2, 0 0)), ((3 1, 3 3, 1 3, 1 2, 2 2, 2 1, 3 1)))')) FROM onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_SymmetricDiff(ST_Point(0,0), null) from onerow",
//			result = "null"
//			)
//		}
//	)

public class ST_SymmetricDiff extends ST_GeometryProcessing {

  static final Logger LOG = LoggerFactory.getLogger(ST_SymmetricDiff.class.getName());

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

    try {
      OGCGeometry diffGeometry = ogcGeom1.symDifference(ogcGeom2);
      return GeometryUtils.geometryToEsriShapeBytesWritable(diffGeometry);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_SymmetricDiff: " + e);
      return null;
    }
  }

}
