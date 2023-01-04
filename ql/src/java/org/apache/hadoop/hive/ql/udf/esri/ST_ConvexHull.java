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
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.udf.esri.GeometryUtils.OGCType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_ConvexHull",
    value = "_FUNC_(ST_Geometry, ST_Geometry, ...) - returns an ST_Geometry as the convex hull of the supplied ST_Geometries",
    extended = "Example: SELECT ST_AsText(ST_ConvexHull(ST_Point(0, 0), ST_Point(0, 1), ST_Point(1, 1))) FROM onerow;\n"
        + "MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsText(ST_ConvexHull(ST_Point(0, 0), ST_Point(0, 1), ST_Point(1, 1))) FROM onerow",
//			result = "MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))"
//			)
//		}
//	)

public class ST_ConvexHull extends ST_GeometryProcessing {

  static final Logger LOG = LoggerFactory.getLogger(ST_ConvexHull.class.getName());

  public BytesWritable evaluate(BytesWritable... geomrefs) {

    // validate arguments
    if (geomrefs == null || geomrefs.length < 1) {
      // LogUtils.Log_VariableArgumentLength(LOG);
      return null;
    }

    int firstWKID = 0;

    // validate spatial references and geometries first
    for (int i = 0; i < geomrefs.length; i++) {

      BytesWritable geomref = geomrefs[i];

      if (geomref == null || geomref.getLength() == 0) {
        LogUtils.Log_ArgumentsNull(LOG);
        return null;
      }

      if (i == 0) {
        firstWKID = GeometryUtils.getWKID(geomref);
      } else if (firstWKID != GeometryUtils.getWKID(geomref)) {
        LogUtils.Log_SRIDMismatch(LOG, geomrefs[0], geomref);
        return null;
      }
    }

    // now build geometry array to pass to GeometryEngine.union
    Geometry[] geomsToProcess = new Geometry[geomrefs.length];

    for (int i = 0; i < geomrefs.length; i++) {
      //HiveGeometry hiveGeometry = GeometryUtils.geometryFromEsriShape(geomrefs[i]);
      OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomrefs[i]);

      if (ogcGeometry == null) {
        LogUtils.Log_ArgumentsNull(LOG);
        return null;
      }

      geomsToProcess[i] = ogcGeometry.getEsriGeometry();
    }

    try {

      Geometry[] geomResult = GeometryEngine.convexHull(geomsToProcess, true);

      if (geomResult.length != 1) {
        return null;
      }

      Geometry merged = geomResult[0];

      // we have to infer the type of the differenced geometry because we don't know
      // if it's going to end up as a single or multi-part geometry
      OGCType inferredType = GeometryUtils.getInferredOGCType(merged);

      return GeometryUtils.geometryToEsriShapeBytesWritable(merged, firstWKID, inferredType);
    } catch (Exception e) {
      LogUtils.Log_ExceptionThrown(LOG, "GeometryEngine.convexHull", e);
      return null;
    }
  }
}
